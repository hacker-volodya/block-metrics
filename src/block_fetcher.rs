pub struct BlockFetcher<S> {
    subscriber: S,
    config: ConfigGlobal,
}

impl BlockFetcher {
    pub fn new(config: ConfigGlobal, subscriber: S) -> Self {
        Self { config, subscriber }
    }

    pub fn start() {

    }
}

struct Node {
    
}

impl Node {
    pub fn fetch_loop() {
        
    }
}