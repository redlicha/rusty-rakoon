// Copyright (C) 2016-2020 Arne Redlich <arne.redlich@googlemail.com>
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

#[macro_use] extern crate lazy_static;
#[macro_use] extern crate log;

#[cfg(test)]
mod test {
    use bytes::BytesMut;
    use env_logger;
    use rand::{
        thread_rng,
        distributions::{
            Sample,
            Range,
        },
    };
    use rusty_rakoon::*;
    use std::{
        self,
        collections::{
            BTreeMap,
            HashSet
        },
        fmt::{
            Display,
        },
        fs::{
            File,
        },
        io::{
            BufWriter,
            Write,
        },
        path::{
            Path,
            PathBuf,
        },
        rc::{
            Rc,
        },
        str,
        sync::{
            Arc,
            Mutex,
        },
        time::{
            Duration,
        },
    };

    use uuid;

    fn get_env_or_default<T: str::FromStr + Clone>(name : &str, default : &T) -> T {
        if let Some(s) = std::env::var_os(name) {
            if let Ok(v) = s.to_str().unwrap().parse::<T>() {
                return v
            }
        }

        default.clone()
    }

    fn hostname() ->&'static str {
        "127.0.0.1"
    }

    struct PortAllocator {
        next_port: u16,
        cache: HashSet<u16>,
    }

    impl PortAllocator {
        fn new(port_base: u16) -> Self {
            PortAllocator{next_port: port_base,
                          cache: HashSet::<u16>::new()}
        }

        fn get(&mut self) -> u16 {
            if self.cache.is_empty() {
                let p = self.next_port;
                self.next_port += 1;
                p
            } else {
                let p = *self.cache.iter().next().unwrap();
                self.cache.remove(&p);
                p
            }
        }

        fn put(&mut self, port: u16) {
            assert!(self.cache.insert(port))
        }
    }

    lazy_static! {
        static ref PORT_ALLOCATOR: Arc<Mutex<PortAllocator>> = {
            let port_base = get_env_or_default("ARAKOON_PORT_BASE",
                                               &17_000);
            Arc::new(Mutex::new(PortAllocator::new(port_base)))
        };
    }

    #[derive(Eq, Hash, PartialEq)]
    struct Port(u16);

    impl Port {
        fn new() -> Port {
            Port(PORT_ALLOCATOR.clone().lock().unwrap().get())
        }
    }

    impl Drop for Port {
        fn drop(&mut self) {
            PORT_ALLOCATOR.clone().lock().unwrap().put(self.0)
        }
    }

    impl Display for Port {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            self.0.fmt(f)
        }
    }

    pub struct ArakoonNode {
        node_id: NodeId,
        client_port: Port,
        messaging_port: Port,
        home: PathBuf,
        binary: PathBuf,
        child: Option<std::process::Child>,
    }

    impl ArakoonNode {
        fn new(node_id : &NodeId,
               root : &PathBuf,
               binary : &PathBuf) -> ArakoonNode {
            let home = ArakoonNode::node_home(root,
                                              node_id);
            std::fs::create_dir_all(&home).unwrap();
            ArakoonNode {node_id: node_id.clone(),
                         client_port: Port::new(),
                         messaging_port: Port::new(),
                         home,
                         binary : binary.clone(),
                         child : None }
        }

        fn node_home(root : &PathBuf, node_id : &NodeId) -> PathBuf {
            root.join(&node_id.to_string())
        }

        fn config_file(&self) -> PathBuf {
            self.home.join("config")
        }

        fn address(&self) -> String {
            hostname().to_owned() + &":".to_owned() + &self.client_port.to_string()
        }

        fn config(&self) -> NodeConfig {
            NodeConfig::new(self.node_id.clone(),
                            &self.address()).expect("fix yer test")
        }

        fn wait_for_service(&self, retries : usize) -> () {
            for i in 0..(retries + 1) {
                let res = std::net::TcpStream::connect(&self.address() as &str);
                match res {
                    Ok(_) => {
                        info!("{} found running after {} retries",
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
            if self.child.is_none() {
                let c = std::process::Command::new(&self.binary)
                    .arg("--node")
                    .arg(&self.node_id.to_string())
                    .arg("-config")
                    .arg(self.config_file())
                    .spawn()
                    .unwrap_or_else(|e| {
                        panic!("failed to fork off {} as {}: {}",
                               self.binary.to_str().unwrap(),
                               self.node_id,
                               e)
                    });
                info!("forked off process for {}: {}",
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
                c.wait().unwrap_or_else(|e| {
                    panic!("failed to wait for child {}: {}", c.id(), e);
                });
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
        nodes : BTreeMap<NodeId, ArakoonNode>,
    }

    impl ArakoonCluster {
        fn new(count : u16) -> ArakoonCluster {
            let tempdir = get_env_or_default("TEMP",
                                             &"/tmp".to_owned());
            let binary = get_env_or_default("ARAKOON_BINARY",
                                            &"/usr/bin/arakoon".to_owned());

            let cluster_id = ClusterId(uuid::Uuid::new_v4().hyphenated().to_string());
            let home = Path::new(&tempdir).join("RustyRakoonTest").join(&cluster_id.to_string());

            info!("setting up arakoon cluster {:?}: {} node(s), home={}, binary={}",
                  cluster_id,
                  count,
                  home.display(),
                  binary);

            if home.exists() {
                std::fs::remove_dir_all(&home).expect("failed to remove test directory");
            }

            let mut cluster = ArakoonCluster{cluster_id: cluster_id.clone(),
                                             home,
                                             nodes: BTreeMap::new()};

            std::fs::create_dir_all(&cluster.home).expect("failed to create test directory");

            for i in 0..count {
                let node_id = NodeId("node_".to_owned() + &i.to_string());
                let node = ArakoonNode::new(&node_id,
                                            &cluster.home,
                                            &PathBuf::from(&binary));
                cluster.nodes.insert(node_id,
                                     node);
            }

            for node in cluster.nodes.values() {
                cluster.write_config_file(node).unwrap();
            }

            for node in cluster.nodes.values_mut() {
                node.start();
            }

            for node in cluster.nodes.values() {
                node.wait_for_service(60);
            }

            cluster
        }

        fn write_config_file(&self, node : &ArakoonNode) -> std::io::Result<()> {
            let f = File::create(node.config_file())?;
            let mut w = BufWriter::new(f);

            writeln!(w, "[global]")?;
            writeln!(w, "cluster_id = {}", self.cluster_id)?;
            write!(w, "cluster = ")?;

            let mut comma = false;
            for node in self.nodes.values() {
                if comma {
                    write!(w, ", ")?;
                } else {
                    comma = true;
                }
                write!(w, "{}", node.node_id)?
            }
            writeln!(w, "")?;

            let (_, master) = self.nodes.iter().next().unwrap();
            writeln!(w, "master = {}", master.node_id)?;
            writeln!(w, "preferred_master = true")?;
            writeln!(w, "")?;

            for node in self.nodes.values() {
                writeln!(w, "[{}]", node.node_id)?;
                writeln!(w, "ip = {}", hostname())?;
                writeln!(w, "client_port = {}", node.client_port)?;
                writeln!(w, "messaging_port = {}", node.messaging_port)?;
                writeln!(w, "log_level = debug")?;
                writeln!(w, "log_dir = {}", node.home.to_str().unwrap())?;
                writeln!(w, "home = {}", node.home.to_str().unwrap())?;
            }

            Ok(())
        }

        fn node_configs(&self) -> Vec<NodeConfig> {
            let mut node_configs = vec![];
            for node in self.nodes.values() {
                node_configs.push(node.config())
            }

            node_configs
        }
    }

    impl Drop for ArakoonCluster {
        fn drop(&mut self) {
            info!("tearing down arakoon cluster {:?}", self.cluster_id);
            self.nodes.clear();
            // assert!(std::fs::remove_dir_all(&self.home).is_ok());
        }
    }

    // TODO: convert this to a method on Node
    async fn determine_master(node: &mut Node,
                              wait_secs: u32) -> std::result::Result<NodeId, Error>
    {
        for i in 0..wait_secs {
            let res = node.who_master().await?;
            if let Some(node_id) = res {
                info!("master is {}", node_id);
                return Ok(node_id);
            } else {
                info!("no master yet, attempt {}, wait_secs {} -> going to sleep",
                      i, wait_secs);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }

        Err(Error::IoError(std::io::Error::new(std::io::ErrorKind::Other,
                                               "no master available")))
    }

    async fn connect_to_master(cluster: Rc<ArakoonCluster>,
                               wait_secs: u32) -> std::result::Result<Node, Error>
    {
        let node_configs = cluster.node_configs().clone();
        assert!(!node_configs.is_empty());

        // pick a random node to connect to exercise the path that first connects to a slave
        let mut rng = thread_rng();
        let mut range = Range::new(0, node_configs.len());
        let idx = range.sample(&mut rng);

        debug!("connecting to {}", node_configs[idx].node_id);
        let mut node = Node::connect(cluster.cluster_id.clone(),
                                     &node_configs[idx]).await?;
        let master_id = determine_master(&mut node,
                                         wait_secs).await?;
        debug!("master is {}", master_id);
        if node_configs[idx].node_id == master_id {
            Ok(node)
        } else {
            let maybe_ncfg = node_configs.iter().find(|cfg|
                                                      {
                                                          cfg.node_id == master_id
                                                      });
            if let Some(node_config) = maybe_ncfg {
                debug!("re-connecting to {}", node_config.node_id);
                let node = Node::connect(cluster.cluster_id.clone(),
                                      node_config).await?;
                Ok(node)
            } else {
                let e = std::io::Error::new(std::io::ErrorKind::Other,
                                            "master not found in cluster config");
                Err(Error::from(e))
            }
        }
    }

    struct Fixture {
        cluster: Rc<ArakoonCluster>
    }

    impl Fixture {
        fn new(num_nodes: u16) -> Self {
            // ignore errors caused by multiple invocations
            drop(env_logger::try_init());
            Fixture {
                cluster: Rc::new(ArakoonCluster::new(num_nodes))
            }
        }

        async fn connect_to_master(&self) -> Node {
            connect_to_master(self.cluster.clone(), 30)
                .await
                .expect("failed to connect to master")
        }
    }

    #[tokio::test]
    async fn setup_and_teardown() {
        Fixture::new(3);
    }

    #[tokio::test]
    async fn master() {
        let fixture = Fixture::new(3);
        let cluster = &fixture.cluster;
        let node_configs = &cluster.node_configs();
        assert!(!node_configs.is_empty());
        let mut client = Node::connect(cluster.cluster_id.clone(),
                                       &node_configs[0]).await.unwrap();

        let res = determine_master(&mut client, 30).await;
        assert_eq!(node_configs[0].node_id,
                   res.unwrap());
    }

    #[tokio::test]
    async fn connect_master() {
        let fixture = Fixture::new(3);
        let cluster = fixture.cluster.clone();
        let node_configs = &cluster.node_configs();
        assert!(!node_configs.is_empty());
        let res = connect_to_master(cluster, 30).await;
        assert_eq!(node_configs[0].node_id,
                   res.unwrap().node_id);
    }

    #[tokio::test]
    async fn hello() {
        let fixture = Fixture::new(3);
        let cluster = fixture.cluster.clone();
        let node_configs = &cluster.node_configs();
        assert!(!node_configs.is_empty());
        let mut client = Node::connect(cluster.cluster_id.clone(),
                                       &node_configs[0])
            .await
            .unwrap();
        let res = client.hello().await;
        assert!(res.is_ok());
        info!("hello response: {}", res.unwrap());
    }

    #[tokio::test]
    async fn exists_inexistent() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let res = master.exists(Consistency::Consistent, key).await.unwrap();
        assert_eq!(false, res);
    }

    #[tokio::test]
    async fn get_inexistent() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let res = master.get(Consistency::Consistent, key).await;
        match res {
            Ok(_) => panic!("'get' returned something for an inexistent key: {:?}",
                            res),
            Err(e) => match e {
                Error::ErrorResponse(code, _) => assert_eq!(ErrorCode::NotFound,
                                                            code),
                _ => panic!("'get' for inexistent key yielded unexpected error response {:?}",
                            e),
            },
        }
    }

    #[tokio::test]
    async fn delete_inexistent() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let res = master.delete(key).await;
        match res {
            Ok(_) => panic!("'delete' returned successfully for an inexistent key: {:?}", res),
            Err(e) => match e {
                Error::ErrorResponse(code, _) => assert_eq!(ErrorCode::NotFound,
                                                            code),
                _ => panic!("'delete' for inexistent key yielded unexpected error response {:?}",
                            e),
            },
        }
    }

    #[tokio::test]
    async fn set_and_get() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let val = BytesMut::from(&b"val"[..]);
        master
            .set(key.clone(), val.clone())
            .await
            .expect("'set' returned error");
        let res = master.get(Consistency::Consistent, key)
            .await
            .expect("'get' returned error");
        assert_eq!(val, res);
    }

    #[tokio::test]
    async fn set_and_exists() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let val = BytesMut::from(&b"val"[..]);
        master.set(key.clone(), val).await.expect("'set' returned error");
        let res = master
            .exists(Consistency::Consistent, key)
            .await
            .expect("'exists' returned error");
        assert_eq!(true, res);
    }

    #[tokio::test]
    async fn set_and_delete_and_exists() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let val = BytesMut::from(&b"val"[..]);
        master.set(key.clone(), val).await.expect("'set' returned error");
        master.delete(key.clone()).await.expect("'delete' returned error");
        let res = master.exists(Consistency::Consistent, key).await.expect("'exists' returned error");
        assert_eq!(false, res);
    }

    #[tokio::test]
    async fn test_and_set_inexistent_failure() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let old = BytesMut::from(&b"old"[..]);
        let new = BytesMut::from(&b"new"[..]);

        let res = master
            .test_and_set(key.clone(), Some(old), Some(new))
            .await
            .expect("'test_and_set' for inexistent key yielded error");
        assert_eq!(None, res);
        let res = master.exists(Consistency::Consistent, key)
            .await
            .expect("'exists' returned error for inexistent key");
        assert_eq!(false, res);
    }

    #[tokio::test]
    async fn test_and_set_inexistent_success() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let new = BytesMut::from(&b"new"[..]);
        let res = master
            .test_and_set(key.clone(), None, Some(new.clone()))
            .await
            .expect("'test_and_set' for inexistent key yielded error");
        assert_eq!(None, res);

        let val = master
            .get(Consistency::Consistent, key)
            .await
            .expect("'get' returned error");

        assert_eq!(val, new);
    }

    #[tokio::test]
    async fn test_and_set_existent_success() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let old = BytesMut::from(&b"old"[..]);
        let new = BytesMut::from(&b"new"[..]);

        master
            .set(key.clone(), old.clone())
            .await
            .expect("'set' failed");

        let res = master
            .test_and_set(key.clone(), Some(old.clone()), Some(new.clone()))
            .await
            .expect("'test_and_set' for existent key yielded error");

        assert_eq!(Some(old), res);

        let val = master
            .get(Consistency::Consistent, key)
            .await
            .expect("'get' returned error");
        assert_eq!(new, val);
    }

    #[tokio::test]
    async fn test_and_set_existent_failure() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let exp_old = BytesMut::from(&b"exp_old"[..]);
        let real_old = BytesMut::from(&b"real_old"[..]);
        let new = BytesMut::from(&b"new"[..]);

        master.set(key.clone(), real_old.clone())
            .await
            .expect("'set' failed");

        let res = master
            .test_and_set(key.clone(), Some(exp_old), Some(new))
            .await
            .expect("'test_and_set' for existent key yielded error");

        assert_eq!(Some(real_old.clone()), res);

        let val = master
            .get(Consistency::Consistent, key)
            .await
            .expect("'get' returned error");

        assert_eq!(real_old, val);
    }

    #[tokio::test]
    async fn sequence_assert_exists_failure() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let res = master.sequence(vec![Action::AssertExists{key}]).await;
        assert_eq!(true, res.is_err());
        let err = res.err().unwrap();
        match err {
            Error::ErrorResponse(code, _) => assert_eq!(ErrorCode::AssertionFailed,
                                                        code),
            _ => panic!("sequence yielded unexpected error {}", err),
        }
    }

    #[tokio::test]
    async fn sequence_assert_exists_success() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let val = BytesMut::from(&b"val"[..]);
        master.set(key.clone(), val).await.expect("'set' failed");
        master
            .sequence(vec![Action::AssertExists{key}])
            .await
            .expect("Sequence[AssertExists] yielded error");
    }

    #[tokio::test]
    async fn sequence_test_and_set_failure() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let val = BytesMut::from(&b"val"[..]);
        let res = master
            .sequence(vec![Action::Assert{key: key.clone(), value: Some(val.clone())},
                           Action::Set{key, value: val}])
            .await;
        assert_eq!(true, res.is_err());
        let err = res.err().unwrap();
        match err {
            Error::ErrorResponse(code, _) => assert_eq!(ErrorCode::AssertionFailed,
                                                        code),
            _ => panic!("sequence yielded unexpected error {}", err),
        }
    }

    #[tokio::test]
    async fn sequence_test_and_set_success() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key = BytesMut::from(&b"key"[..]);
        let val = BytesMut::from(&b"val"[..]);
        master
            .sequence(vec![Action::Assert{key: key.clone(), value: None},
                           Action::Set{key: key.clone(), value: val.clone()}])
            .await
            .expect("'sequence' failed");

        let res = master
            .get(Consistency::Consistent, key)
            .await
            .expect("'get' failed");
        assert_eq!(res, val);
    }

    #[tokio::test]
    async fn prefix_keys_failure() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key1 = BytesMut::from(&b"key1"[..]);
        let val1 = BytesMut::from(&b"val1"[..]);
        let key2 = BytesMut::from(&b"key2"[..]);
        let val2 = BytesMut::from(&b"val2"[..]);

        master
            .sequence(vec![Action::Set{key: key1.clone(), value: val1},
                           Action::Set{key: key2.clone(), value: val2}])
            .await
            .expect("'sequence' failed");

        let pfx = BytesMut::from(&b"no_such_prefix"[..]);
        let res = master
            .prefix_keys(Consistency::Consistent, pfx.clone(), 0)
            .await
            .expect("'prefix_keys(0)' failed");
        assert_eq!(0, res.len());

        let res = master
            .prefix_keys(Consistency::Consistent, pfx, 10)
            .await
            .expect("'prefix_keys(10)' failed");
        assert_eq!(0, res.len());
    }

    #[tokio::test]
    async fn prefix_keys_success() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key1 = BytesMut::from(&b"key1"[..]);
        let val1 = BytesMut::from(&b"val1"[..]);
        let key2 = BytesMut::from(&b"key2"[..]);
        let val2 = BytesMut::from(&b"val2"[..]);

        master
            .sequence(vec![Action::Set{key: key1.clone(), value: val1},
                           Action::Set{key: key2.clone(), value: val2}])
            .await
            .expect("'sequence' failed");

        let pfx = BytesMut::from(&b"key"[..]);
        let vec = master
            .prefix_keys(Consistency::Consistent, pfx.clone(), 0)
            .await
            .expect("'prefix_keys(0)' failed");
        assert_eq!(0, vec.len());

        let vec = master
            .prefix_keys(Consistency::Consistent, pfx.clone(), 1)
            .await
            .expect("'prefix_keys(1)' failed");
        assert_eq!(1, vec.len());
        assert_eq!(key1, vec[0]);

        let vec = master
            .prefix_keys(Consistency::Consistent, pfx.clone(), 10)
            .await
            .expect("'prefix_keys(10)' failed");
        assert_eq!(2, vec.len());
        assert_eq!(key1, vec[0]);
        assert_eq!(key2, vec[1]);
    }

    #[tokio::test]
    async fn range_success() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key1 = BytesMut::from(&b"key1"[..]);
        let val1 = BytesMut::from(&b"val1"[..]);
        let key2 = BytesMut::from(&b"key2"[..]);
        let val2 = BytesMut::from(&b"val2"[..]);

        master
            .sequence(vec![Action::Set{key: key1.clone(), value: val1},
                           Action::Set{key: key2.clone(), value: val2}])
            .await
            .expect("'sequence' failed");

        let vec = master
            .range(Consistency::Consistent,
                   Some(key1.clone()),
                   true,
                   None,
                   true,
                   100)
            .await
            .expect("'range' failed");

        assert_eq!(2, vec.len());
        assert_eq!(key1, vec[0]);
        assert_eq!(key2, vec[1]);
    }

    #[tokio::test]
    async fn range_entries() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key1 = BytesMut::from(&b"key1"[..]);
        let val1 = BytesMut::from(&b"val1"[..]);
        let key2 = BytesMut::from(&b"key2"[..]);
        let val2 = BytesMut::from(&b"val2"[..]);

        master
            .sequence(vec![Action::Set{key: key1.clone(), value: val1.clone()},
                           Action::Set{key: key2.clone(), value: val2.clone()}])
            .await
            .expect("'sequence' failed");

        let vec = master
            .range_entries(Consistency::Consistent,
                           None,
                           true,
                           Some(key2.clone()),
                           true,
                           100)
            .await
            .expect("'range_entries' failed");

        assert_eq!(2, vec.len());
        assert_eq!((key1, val1), vec[0]);
        assert_eq!((key2, val2), vec[1]);
    }

    #[tokio::test]
    async fn delete_prefix() {
        let fixture = Fixture::new(3);
        let mut master = fixture.connect_to_master().await;
        let key1 = BytesMut::from(&b"key1"[..]);
        let val1 = BytesMut::from(&b"val1"[..]);
        let key2 = BytesMut::from(&b"key2"[..]);
        let val2 = BytesMut::from(&b"val2"[..]);

        master
            .sequence(vec![Action::Set{key: key1, value: val1},
                           Action::Set{key: key2, value: val2}])
            .await
            .expect("'sequence' failed");

        let pfx = BytesMut::from(&b"key"[..]);
        let count = master
            .delete_prefix(pfx.clone())
            .await
            .expect("'delete_prefix' failed");

        assert_eq!(2, count);

        let vec = master
            .prefix_keys(Consistency::Consistent, pfx, 100)
            .await
            .expect("'prefix_keys' failed");

        assert_eq!(true, vec.is_empty());
    }
}
