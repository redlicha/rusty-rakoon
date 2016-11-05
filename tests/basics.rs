// Copyright (C) 2016 Arne Redlich <arne.redlich@googlemail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate libc;
extern crate rusty_rakoon;
extern crate uuid;

#[cfg(test)]
mod test {

    use libc;
    use std;
    use std::collections::BTreeMap;
    use std::ffi::{CStr, CString};
    use std::fs::File;
    use std::io::{BufWriter, Write};
    use std::path::{Path, PathBuf};
    use std::str;
    use std::sync::{Arc, Mutex, Once, ONCE_INIT};
    use rusty_rakoon::bits::*;
    use uuid;

    fn getenv(name : &str) -> Option<String> {
        unsafe {
            if let Ok(c) = CString::new(name) {
                let cstr = libc::getenv(c.as_ptr());
                if !cstr.is_null() {
                    if let Ok(s) = CStr::from_ptr(cstr).to_str() {
                        return Some(s.to_owned())
                    }
                }
            }
            None
        }
    }

    fn get_env_or_default<T: str::FromStr + Clone>(name : &str, default : &T) -> T {
        if let Some(s) = getenv(name) {
            if let Ok(v) = s.parse::<T>() {
                return v
            }
        }

        default.clone()
    }

    fn hostname() -> String {
        "127.0.0.1".to_owned()
    }

    // Singleton as tests can run in parallel. For now ports are not recycled - look into this once
    // the number of tests grows.
    // Cribbed from http://stackoverflow.com/questions/27791532/how-do-i-create-a-global-mutable-singleton
    #[derive(Clone)]
    struct PortAllocator {
        next_port : Arc<Mutex<u16>>,
    }

    fn port_allocator() -> PortAllocator {
        static mut SINGLETON : *const PortAllocator = 0 as *const PortAllocator;
        static ONCE : Once = ONCE_INIT;

        unsafe {
            ONCE.call_once(|| {

                let port_base = get_env_or_default("ARAKOON_PORT_BASE",
                                                   &17000);
                let singleton = PortAllocator {
                    next_port : Arc::new(Mutex::new(port_base))
                };

                SINGLETON = std::mem::transmute(Box::new(singleton));

                // free heap memory at exit
                // This doesn't exist in stable rust yet, so we will just leak it!
                // rt::at_exit(|| {
                //     let singleton: Box<PortAllocator> = std::mem::transmute(SINGLETON);
                //     drop(singleton);

                //     // Set it to null again. I hope only one thread can call `at_exit`!
                //     SINGLETON = 0 as *const _;
                // });
            });

            // give out a copy of the data that is safe to use concurrently.
            (*SINGLETON).clone()
        }
    }

    pub struct ArakoonNode {
        node_id : NodeId,
        port_base : u16,
        home : PathBuf,
        binary : PathBuf,
        child : Option<std::process::Child>,
    }

    impl ArakoonNode {
        fn new(node_id : NodeId,
               port_base : u16,
               root : &PathBuf,
               binary : &PathBuf) -> ArakoonNode {
            let home = ArakoonNode::node_home(root,
                                              &node_id);
            std::fs::create_dir_all(&home).unwrap();
            ArakoonNode {node_id : node_id.clone(),
                         port_base : port_base,
                         home : home,
                         binary : binary.clone(),
                         child : None }
        }

        fn node_home(root : &PathBuf, node_id : &NodeId) -> PathBuf {
            root.join(&node_id.to_string())
        }

        fn client_port(&self) -> u16 {
            self.port_base
        }

        fn messaging_port(&self) -> u16 {
            self.port_base + 1
        }

        fn config_file(&self) -> PathBuf {
            self.home.join("config")
        }

        fn address(&self) -> String {
            hostname() + &":".to_owned() + &self.client_port().to_string()
        }
        fn config(&self) -> NodeConfig {
            NodeConfig::new(&self.node_id,
                            &self.address())
        }

        fn wait_for_service(&self, retries : usize) -> () {
            for i in 0..(retries + 1) {
                let res = std::net::TcpStream::connect(&self.address() as &str);
                match res {
                    Ok(_) => {
                        println!("{} found running after {} retries",
                                 self.node_id,
                                 i);
                        return ();
                    },
                    Err(e) => if i == retries {
                        panic!("{} still not found running after {} retries: {}",
                               self.node_id,
                               i,
                               e);
                    } else {
                        std::thread::sleep(std::time::Duration::new(1, 0));
                    }
                }
            }
        }

        fn start(&mut self) {
            if let None = self.child {
                let c = std::process::Command::new(&self.binary)
                    .arg("--node")
                    .arg(&self.node_id.to_string())
                    .arg("-config")
                    .arg(self.config_file())
                    .spawn()
                    .unwrap_or_else(|e| {panic!("failed to fork off {} as {}: {}",
                                                self.binary.to_str().unwrap(),
                                                self.node_id,
                                                e)});
                println!("forked off process for {}: {}",
                         self.node_id,
                         c.id());
                self.child = Some(c);
            } else {
                panic!("{} is already running!", self.node_id)
            }
        }

        fn stop(&mut self) {
            if let Some(ref mut c) = self.child {
                c.kill().unwrap(); // FIXME: sends SIGKILL which prevents an orderly shutdown
                c.wait().unwrap_or_else(|e| {panic!("failed to wait for child {}: {}",
                                                    c.id(),
                                                    e); });
            }

            self.child = None;

        }
    }

    impl Drop for ArakoonNode {
        fn drop(&mut self) {
            self.stop();
        }
    }

    pub struct ArakoonCluster {
        cluster_id : ClusterId,
        home : PathBuf,
        port_base : u16,
        nodes : BTreeMap<NodeId, ArakoonNode>,
    }

    type ArakoonConnection = Connection<std::net::TcpStream>;

    impl ArakoonCluster {
        fn new(count : u16) -> ArakoonCluster {
            let tempdir = get_env_or_default("TEMP",
                                             &"/tmp".to_owned());
            let binary = get_env_or_default("ARAKOON_BINARY",
                                            &"/usr/bin/arakoon".to_owned());

            let cluster_id = ClusterId(uuid::Uuid::new_v4().hyphenated().to_string());
            let home = Path::new(&tempdir).join("RustyRakoonTest").join(&cluster_id.to_string());
            let port_allocator = port_allocator();
            let mut next_port = port_allocator.next_port.lock().unwrap();
            let port_base = *next_port;
            *next_port += 2 * count;

            println!("setting up arakoon cluster {:?}: {} node(s), home={}, binary={}, port_base={}",
                     cluster_id,
                     count,
                     home.display(),
                     binary,
                     port_base);

            if home.exists() {
                assert!(std::fs::remove_dir_all(&home).is_ok());
            }

            let mut cluster = ArakoonCluster{cluster_id : cluster_id.clone(),
                                             home : home,
                                             port_base : port_base,
                                             nodes : BTreeMap::new() };

            assert!(std::fs::create_dir_all(&cluster.home).is_ok());

            for i in 0..count {
                let node_id = NodeId("node_".to_owned() + &i.to_string());
                let node = ArakoonNode::new(node_id.clone(),
                                            cluster.port_base + 2 * i,
                                            &cluster.home,
                                            &PathBuf::from(&binary));
                cluster.nodes.insert(node_id,
                                     node);
            }

            for (_, node) in cluster.nodes.iter() {
                cluster.write_config_file(&node).unwrap();
            }

            for (_, node) in cluster.nodes.iter_mut() {
                node.start();
            }

            for (_, node) in cluster.nodes.iter() {
                node.wait_for_service(60);
            }

            cluster
        }

        fn write_config_file(&self, node : &ArakoonNode) -> std::io::Result<()> {
            let f = try!(File::create(node.config_file()));
            let mut w = BufWriter::new(f);

            try!(writeln!(w, "[global]"));
            try!(writeln!(w, "cluster_id = {}", self.cluster_id));
            try!(write!(w, "cluster = "));

            let mut comma = false;
            for (_, node) in self.nodes.iter() {
                if comma {
                    try!(write!(w, ", "));
                } else {
                    comma = true;
                }
                try!(write!(w, "{}", node.node_id))
            }
            try!(writeln!(w, ""));

            let (_, master) = self.nodes.iter().next().unwrap();
            try!(writeln!(w, "master = {}", master.node_id));
            try!(writeln!(w, "preferred_master = true"));
            try!(writeln!(w, ""));

            for (_, node) in self.nodes.iter() {
                try!(writeln!(w, "[{}]", node.node_id));
                try!(writeln!(w, "ip = {}", hostname()));
                try!(writeln!(w, "client_port = {}", node.client_port()));
                try!(writeln!(w, "messaging_port = {}", node.messaging_port()));
                try!(writeln!(w, "log_level = debug"));
                try!(writeln!(w, "log_dir = {}", node.home.to_str().unwrap()));
                try!(writeln!(w, "home = {}", node.home.to_str().unwrap()));
            }

            Ok(())
        }

        fn node_configs(&self) -> Vec<NodeConfig> {
            let mut node_configs = vec![];
            for (_, node) in self.nodes.iter() {
                node_configs.push(node.config())
            }

            node_configs
        }

        fn connect_to_node(&self, node_config : &NodeConfig) -> Result<ArakoonConnection> {
            match std::net::TcpStream::connect(&node_config.addr as &str) {
                Ok(stream) => {
                    let mut conn = Connection::new(stream,
                                                   &self.cluster_id,
                                                   &node_config.id);
                    match conn.prologue() {
                        Ok(_) => Ok(conn),
                        Err(e) => Err(Error::IoError(e)),
                    }
                },
                Err(e) => {
                    Err(Error::IoError(e))
                }
            }
        }

        fn determine_master(&self, wait_secs : u32) -> ArakoonConnection {
            assert!(wait_secs > 0);
            for i in 0..wait_secs {
                if let Ok(mut conn) = self.connect_to_node(&self.node_configs()[0]) {
                    if conn.who_master_req().is_ok() {
                        if let Ok(Some(master)) = conn.who_master_rsp() {
                            println!("master: {:?}", master);
                            if let Ok(master_conn) = self.connect_to_node(&self.nodes.get(&master).unwrap().config()) {
                                return master_conn;
                            }
                        }
                    }
                }
                println!("failed to determine master {} - going to sleep for a second", i);
                std::thread::sleep(std::time::Duration::new(1, 0));
            }

            panic!("could not determine master after {} seconds",
                   wait_secs);
        }
    }

    impl Drop for ArakoonCluster {
        fn drop(&mut self) {
            println!("tearing down arakoon cluster {:?}", self.cluster_id);
            self.nodes.clear();
            assert!(std::fs::remove_dir_all(&self.home).is_ok());
        }
    }

    fn req_rsp<ReqFn, RspFn, Ret>(conn : &mut ArakoonConnection,
                                  req_fn : ReqFn,
                                  rsp_fn : RspFn) -> Ret
        where ReqFn : Fn(&mut ArakoonConnection) -> std::io::Result<()>,
    RspFn : Fn(&mut ArakoonConnection) -> Result<Ret> {
        assert!(req_fn(conn).is_ok());
        let rep = rsp_fn(conn);
        assert!(rep.is_ok());
        rep.unwrap()
    }

    fn exists_(conn : &mut ArakoonConnection, key : &[u8]) -> bool {
        req_rsp(conn,
                |conn| { conn.exists_req(&Consistency::Consistent, key) },
                |conn| { conn.exists_rsp() })
    }

    fn get_(conn : &mut ArakoonConnection,
            key : &[u8]) -> Option<Vec<u8>>
    {
        assert!(conn.get_req(&Consistency::Consistent,
                             key).is_ok());
        let rep = conn.get_rsp();
        match rep {
            Ok(val) => Some(val),
            Err(Error::ErrorResponse(ErrorCode::NotFound, _)) => None,
            Err(e) => panic!("unexpected error {:?}", e)
        }
    }

    fn set_(conn : &mut ArakoonConnection,
            key : &[u8],
            val : &[u8])
    {
        req_rsp(conn,
                |conn| { conn.set_req(key, val) },
                |conn| { conn.set_rsp() })
    }

    fn delete_(conn : &mut ArakoonConnection,
               key : &[u8]) -> bool
    {
        assert!(conn.delete_req(key).is_ok());
        let rep = conn.delete_rsp();
        match rep {
            Ok(()) => true,
            Err(Error::ErrorResponse(ErrorCode::NotFound, _)) => false,
            _ => panic!("unexpected DELETE reply {:?}", rep)
        }
    }

    fn make_keys_and_vals(count : usize, key_pfx : &str, val_pfx : &str) -> KeysAndVals {
        let mut vec = Vec::new();
        vec.reserve(count);

        for i in 0..count {
            let k = key_pfx.to_owned() + &i.to_string();
            let v = val_pfx.to_owned() + &i.to_string();

            vec.push((k.as_bytes().to_owned(),
                      v.as_bytes().to_owned()));
        }
        vec
    }

    fn set_keys_and_vals(conn : &mut ArakoonConnection, kvs : &KeysAndVals) {
        for &(ref k, ref v) in kvs.iter() {
            set_(conn, k, v);
        }
    }

    fn keys(kvs : &KeysAndVals) -> Vec<Vec<u8>> {
        kvs.iter().map(|&(ref k, _)|{ k.clone() }).collect()
    }

    fn fill(conn : &mut ArakoonConnection,
            count : usize,
            key_pfx : &str,
            val_pfx : &str) -> KeysAndVals {
        let kvs = make_keys_and_vals(count, key_pfx, val_pfx);
        set_keys_and_vals(conn, &kvs);
        kvs
    }

    fn range_(conn : &mut ArakoonConnection,
              first : Option<&[u8]>,
              include_first : bool,
              last : Option<&[u8]>,
              include_last : bool,
              max : i32) -> Vec<Vec<u8>>
    {
        req_rsp(conn,
                |conn| { conn.range_req(&Consistency::Consistent,
                                        first,
                                        include_first,
                                        last,
                                        include_last,
                                        max) },
                |conn| { conn.range_rsp() })
    }


    fn range_entries_(conn : &mut ArakoonConnection,
                      first : Option<&[u8]>,
                      include_first : bool,
                      last : Option<&[u8]>,
                      include_last : bool,
                      max : i32) -> KeysAndVals
    {
        req_rsp(conn,
                |conn| { conn.range_entries_req(&Consistency::Consistent,
                                                first,
                                                include_first,
                                                last,
                                                include_last,
                                                max) },
                |conn| { conn.range_entries_rsp() })
    }

    fn prefix_keys_(conn : &mut ArakoonConnection, pfx : &[u8], max : i32) -> Vec<Vec<u8>> {
        req_rsp(conn,
                |conn| { conn.prefix_keys_req(&Consistency::Consistent,
                                              pfx,
                                              max) },
                |conn| { conn.prefix_keys_rsp() })
    }

    fn test_and_set_(conn : &mut ArakoonConnection,
                     key : &[u8],
                     old : Option<&[u8]>,
                     new : Option<&[u8]>) -> Option<Vec<u8>> {
        req_rsp(conn,
                |conn| { conn.test_and_set_req(key,
                                               old,
                                               new) },
                |conn| { conn.test_and_set_rsp() })
    }

    fn delete_prefix_(conn : &mut ArakoonConnection,
                      pfx : &[u8]) -> i32 {
        req_rsp(conn,
                |conn| { conn.delete_prefix_req(pfx) },
                |conn| { conn.delete_prefix_rsp() })
    }

    fn seq_req_rsp<ReqFn, RspFn>(conn : &mut ArakoonConnection,
                                 acts : &[Action],
                                 req_fn : ReqFn,
                                 rsp_fn : RspFn) -> bool
        where ReqFn : Fn(&mut ArakoonConnection, &[Action]) -> std::io::Result<()>,
    RspFn : Fn(&mut ArakoonConnection) -> Result<()> {
        assert!(req_fn(conn, acts).is_ok());
        let rep = rsp_fn(conn);
        match rep {
            Ok(()) => true,
            Err(Error::ErrorResponse(ErrorCode::AssertionFailed, _)) => false,
            Err(e) => panic!("unexpected error {:?}", e)
        }
    }

    fn sequence_(conn : &mut ArakoonConnection,
                 acts : &[Action]) -> bool
    {
        seq_req_rsp(conn,
                    acts,
                    |conn, acts| { conn.sequence_req(acts) },
                    |conn | { conn.sequence_rsp() })
    }

    fn synced_sequence_(conn : &mut ArakoonConnection,
                        acts : &[Action]) -> bool {
        seq_req_rsp(conn,
                    acts,
                    |conn, acts| { conn.synced_sequence_req(acts) },
                    |conn| { conn.synced_sequence_rsp() })
    }

    fn test_sequence<SeqFn>(seq_fn : SeqFn)
        where SeqFn : Fn(&mut ArakoonConnection, &[Action]) -> bool {

        let cluster = ArakoonCluster::new(1);
        let kvs = make_keys_and_vals(1, "key", "val");
        let mut conn = cluster.determine_master(60);

        set_keys_and_vals(&mut conn, &kvs);

        let key = "foo".as_bytes();
        let val = "bar".as_bytes();

        let seq = vec![ Action::AssertExists{key : key},
                        Action::Assert{key : key, value : Some(val)},
                        Action::Set{key : &kvs[0].0, value : val},
                        Action::Delete{key : key } ];

        assert_eq!(false,
                   seq_fn(&mut conn,
                          &seq));

        assert_eq!(kvs,
                   range_entries_(&mut conn,
                                  None,
                                  false,
                                  None,
                                  false,
                                  -1));

        set_(&mut conn,
             key,
             val);

        assert_eq!(true,
                   seq_fn(&mut conn,
                          &seq));

        assert_eq!(vec![(kvs[0].0.clone(), val.to_owned())],
                   range_entries_(&mut conn,
                                  None,
                                  false,
                                  None,
                                  false,
                                  -1));
    }

    #[test]
    fn setup_and_teardown() {
        let _ = ArakoonCluster::new(3);
        // std::thread::sleep(std::time::Duration::new(300, 0));
    }

    #[test]
    fn master() {
        let cluster = ArakoonCluster::new(3);

        let node_configs = cluster.node_configs();
        assert_eq!(3, node_configs.len());

        let conn = cluster.determine_master(60);

        assert_eq!(conn.node_id,
                   node_configs[0].id);
    }

    #[test]
    fn hello() {
        let cluster = ArakoonCluster::new(3);

        let node_configs = cluster.node_configs();
        assert_eq!(3, node_configs.len());

        let mut conn = cluster.connect_to_node(&node_configs[2]).unwrap();
        assert!(conn.hello_req("Hullo, it is I").is_ok());
        let rep = conn.hello_rsp();
        println!("got reply {}", std::str::from_utf8(&rep.unwrap()).unwrap());
    }

    #[test]
    fn inexistence() {
        let cluster = ArakoonCluster::new(1);
        let mut conn = cluster.determine_master(60);
        let kvs = make_keys_and_vals(1, "key", "val");

        assert_eq!(false,
                   exists_(&mut conn,
                           &kvs[0].0));

        assert!(get_(&mut conn,
                     &kvs[0].0).is_none());
    }

    #[test]
    fn set_get_delete() {
        let cluster = ArakoonCluster::new(1);

        let key = "key".as_bytes();

        let mut conn = cluster.determine_master(60);

        assert_eq!(false,
                   exists_(&mut conn,
                           &key));

        let old_val = "old_val".as_bytes();

        set_(&mut conn,
             &key,
             &old_val);

        assert!(exists_(&mut conn,
                        &key));


        assert_eq!(old_val,
                   get_(&mut conn,
                        &key).unwrap().as_slice());

        let new_val = "new val".as_bytes();

        set_(&mut conn,
             &key,
             &new_val);

        assert_eq!(new_val,
                   get_(&mut conn,
                        &key).unwrap().as_slice());

        assert!(delete_(&mut conn,
                        &key));

        assert_eq!(false,
                   exists_(&mut conn,
                           &key));

        assert_eq!(false,
                   delete_(&mut conn,
                           &key));
    }

    #[test]
    fn range() {
        let cluster = ArakoonCluster::new(1);
        let kvs = make_keys_and_vals(2, "key", "val");

        let mut conn = cluster.determine_master(60);

        assert!(range_(&mut conn,
                       Some(&kvs[0].0),
                       true,
                       Some(&kvs[1].0),
                       true,
                       2).is_empty());

        set_keys_and_vals(&mut conn,
                          &kvs);

        assert!(range_(&mut conn,
                       Some(&kvs[0].0),
                       false,
                       Some(&kvs[1].0),
                       false,
                       2).is_empty());

        assert_eq!(vec![ kvs[0].0.clone(), kvs[1].0.clone() ],
                   range_(&mut conn,
                          Some(&kvs[0].0),
                          true,
                          Some(&kvs[1].0),
                          true,
                          -1));

        assert_eq!(vec![ kvs[0].0.clone() ],
                   range_(&mut conn,
                          Some(&kvs[0].0),
                          true,
                          Some(&kvs[1].0),
                          false,
                          -1));

        assert_eq!(vec![ kvs[1].0.clone() ],
                   range_(&mut conn,
                          Some(&kvs[0].0),
                          false,
                          Some(&kvs[1].0),
                          true,
                          -1));

        assert_eq!(vec![ kvs[0].0.clone(), kvs[1].0.clone() ],
                   range_(&mut conn,
                          None,
                          false,
                          None,
                          false,
                          -1));

        assert_eq!(vec![ kvs[0].0.clone() ],
                   range_(&mut conn,
                          None,
                          false,
                          None,
                          false,
                          1));

        // there's a bug in arakoon versions that might make this one fail
        // as it returns a list with 1 element even though the max is set
        // to 0 - https://github.com/openvstorage/arakoon/issues/68
        assert!(range_(&mut conn,
                       None,
                       false,
                       None,
                       false,
                       0).is_empty());
    }

    #[test]
    fn range_entries() {
        let cluster = ArakoonCluster::new(1);
        let kvs = make_keys_and_vals(2, "key", "val");

        let mut conn = cluster.determine_master(60);

        assert!(range_entries_(&mut conn,
                               Some(&kvs[0].0),
                               true,
                               Some(&kvs[0].1),
                               true,
                               -1).is_empty());

        set_keys_and_vals(&mut conn, &kvs);

        assert!(range_entries_(&mut conn,
                               Some(&kvs[0].0),
                               false,
                               Some(&kvs[1].0),
                               false,
                               -1).is_empty());

        assert_eq!(kvs,
                   range_entries_(&mut conn,
                                  Some(&kvs[0].0),
                                  true,
                                  Some(&kvs[1].0),
                                  true,
                                  -1).as_slice());

        assert_eq!(vec![ kvs[0].clone() ],
                   range_entries_(&mut conn,
                                  Some(&kvs[0].0),
                                  true,
                                  Some(&kvs[1].0),
                                  false,
                                  -1));

        assert_eq!(vec![ kvs[1].clone() ],
                   range_entries_(&mut conn,
                                  Some(&kvs[0].0),
                                  false,
                                  Some(&kvs[1].0),
                                  true,
                                  -1));

        assert_eq!(kvs,
                   range_entries_(&mut conn,
                                  None,
                                  false,
                                  None,
                                  false,
                                  -1).as_slice());

        assert_eq!(vec![ kvs[0].clone() ],
                   range_entries_(&mut conn,
                                  None,
                                  false,
                                  None,
                                  false,
                                  1));

        assert!(range_entries_(&mut conn,
                               None,
                               false,
                               None,
                               false,
                               0).is_empty());
    }

    #[test]
    fn prefix_keys() {
        let cluster = ArakoonCluster::new(1);
        let kvs1 = make_keys_and_vals(2, "key", "val");
        let kvs2 = make_keys_and_vals(2, "kex", "val");

        let mut conn = cluster.determine_master(60);

        assert!(prefix_keys_(&mut conn,
                             "ke".as_bytes(),
                             -1).is_empty());

        set_keys_and_vals(&mut conn, &kvs1);
        set_keys_and_vals(&mut conn, &kvs2);

        let ks1 = keys(&kvs1);
        assert_eq!(ks1,
                   prefix_keys_(&mut conn,
                                "key".as_bytes(),
                                -1));

        let ks2 = keys(&kvs2);
        assert_eq!(ks2,
                   prefix_keys_(&mut conn,
                                "kex".as_bytes(),
                                -1));

        let mut ks = ks2.clone();
        ks.append(&mut ks1.clone());

        assert_eq!(ks,
                   prefix_keys_(&mut conn,
                                "ke".as_bytes(),
                                -1));

        assert_eq!(ks2,
                   prefix_keys_(&mut conn,
                                "ke".as_bytes(),
                                ks2.len() as i32));

        assert!(prefix_keys_(&mut conn,
                             "ke".as_bytes(),
                             0).is_empty());

        assert!(prefix_keys_(&mut conn,
                             "hey".as_bytes(),
                             -1).is_empty());
    }

    #[test]
    fn delete_prefix() {
        let cluster = ArakoonCluster::new(1);
        let kvs1 = make_keys_and_vals(2, "key", "val");
        let kvs2 = make_keys_and_vals(2, "kex", "val");

        let mut conn = cluster.determine_master(60);

        assert_eq!(0,
                   delete_prefix_(&mut conn,
                                  "ke".as_bytes()));

        set_keys_and_vals(&mut conn, &kvs1);
        set_keys_and_vals(&mut conn, &kvs2);

        assert_eq!(kvs1.len() as i32,
                   delete_prefix_(&mut conn, "key".as_bytes()));

        let ks2 = keys(&kvs2);
        assert_eq!(ks2,
                   range_(&mut conn,
                          None,
                          false,
                          None,
                          false,
                          -1));

        assert_eq!(kvs2.len() as i32,
                   delete_prefix_(&mut conn, "".as_bytes()));

        assert!(range_(&mut conn,
                       None,
                       false,
                       None,
                       false,
                       -1).is_empty());
    }

    #[test]
    fn test_and_set() {
        let cluster = ArakoonCluster::new(1);
        let kvs = make_keys_and_vals(1, "key", "val");
        let mut conn = cluster.determine_master(60);

        assert!(test_and_set_(&mut conn,
                              &kvs[0].0,
                              Some(&kvs[0].1),
                              None).is_none());

        set_keys_and_vals(&mut conn, &kvs);

        let val = "zal".as_bytes();

        assert_eq!(kvs[0].1,
                   test_and_set_(&mut conn,
                                 &kvs[0].0,
                                 Some(val),
                                 Some(val)).unwrap());

        assert_eq!(kvs[0].1,
                   test_and_set_(&mut conn,
                                 &kvs[0].0,
                                 Some(&kvs[0].1),
                                 Some(val)).unwrap());

        assert_eq!(val,
                   test_and_set_(&mut conn,
                                 &kvs[0].0,
                                 Some(val),
                                 None).unwrap().as_slice());

        assert_eq!(false,
                   exists_(&mut conn,
                           &kvs[0].0));
    }

    #[test]
    fn sequence() {
        test_sequence(|conn, acts| { sequence_(conn, acts) })
    }

    #[test]
    fn synced_sequence() {
        test_sequence(|conn, acts| { synced_sequence_(conn, acts) })
    }
}
