use async_trait::async_trait;

#[derive(Debug)]
pub enum PluginError {
    NotFound(String),
    ExecutionFailed(String),
    InvalidInput(String),
}

impl std::fmt::Display for PluginError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(s) => write!(f, "plugin not found: {}", s),
            Self::ExecutionFailed(s) => write!(f, "execution failed: {}", s),
            Self::InvalidInput(s) => write!(f, "invalid input: {}", s),
        }
    }
}

impl std::error::Error for PluginError {}

#[async_trait]
pub trait Plugin: Send + Sync {
    fn name(&self) -> &str;
    fn version(&self) -> &str;
    async fn execute(&self, command: &str, args: &[Vec<u8>]) -> Result<Vec<u8>, PluginError>;
    fn on_connect(&self, _client_id: u64) {}
    fn on_disconnect(&self, _client_id: u64) {}
}

#[async_trait]
pub trait AuthPlugin: Send + Sync {
    fn name(&self) -> &str;
    async fn authenticate(&self, credentials: &[u8]) -> Result<bool, PluginError>;
}

pub struct PluginManager {
    plugins: Vec<Box<dyn Plugin>>,
    auth_plugins: Vec<Box<dyn AuthPlugin>>,
}

impl PluginManager {
    pub fn new() -> Self {
        Self {
            plugins: Vec::new(),
            auth_plugins: Vec::new(),
        }
    }

    pub fn register_plugin(&mut self, plugin: Box<dyn Plugin>) {
        log::info!("Registered plugin: {}", plugin.name());
        self.plugins.push(plugin);
    }

    pub fn register_auth_plugin(&mut self, plugin: Box<dyn AuthPlugin>) {
        log::info!("Registered auth plugin: {}", plugin.name());
        self.auth_plugins.push(plugin);
    }

    pub fn get_plugin(&self, name: &str) -> Option<&dyn Plugin> {
        self.plugins.iter().find(|p| p.name() == name).map(|p| p.as_ref())
    }

    pub async fn execute_custom_command(&self, command: &str, args: &[Vec<u8>]) -> Result<Vec<u8>, PluginError> {
        for plugin in &self.plugins {
            if let Ok(result) = plugin.execute(command, args).await {
                return Ok(result);
            }
        }
        Err(PluginError::NotFound(format!("no plugin handles command: {}", command)))
    }

    pub async fn check_auth(&self, credentials: &[u8]) -> bool {
        if self.auth_plugins.is_empty() {
            return true;
        }
        for plugin in &self.auth_plugins {
            if let Ok(true) = plugin.authenticate(credentials).await {
                return true;
            }
        }
        false
    }

    pub fn on_connect(&self, client_id: u64) {
        for plugin in &self.plugins {
            plugin.on_connect(client_id);
        }
    }

    pub fn on_disconnect(&self, client_id: u64) {
        for plugin in &self.plugins {
            plugin.on_disconnect(client_id);
        }
    }
}

impl Default for PluginManager {
    fn default() -> Self {
        Self::new()
    }
}

pub struct EchoPlugin;

#[async_trait]
impl Plugin for EchoPlugin {
    fn name(&self) -> &str { "echo" }
    fn version(&self) -> &str { "1.0.0" }

    async fn execute(&self, command: &str, args: &[Vec<u8>]) -> Result<Vec<u8>, PluginError> {
        match command {
            "ECHO" => Ok(args.first().cloned().unwrap_or_default()),
            "REVERSE" => {
                let input = args.first().map(|b| b.as_slice()).unwrap_or(b"");
                let reversed: Vec<u8> = input.iter().rev().cloned().collect();
                Ok(reversed)
            }
            "UPPERCASE" => {
                let input = args.first().map(|b| b.as_slice()).unwrap_or(b"");
                Ok(input.to_ascii_uppercase())
            }
            _ => Err(PluginError::NotFound(format!("unknown command: {}", command))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_echo_plugin() {
        let plugin = EchoPlugin;
        let result = plugin.execute("ECHO", &[b"hello".to_vec()]).await.unwrap();
        assert_eq!(result, b"hello");
    }

    #[tokio::test]
    async fn test_plugin_manager() {
        let mut manager = PluginManager::new();
        manager.register_plugin(Box::new(EchoPlugin));
        let result = manager.execute_custom_command("ECHO", &[b"test".to_vec()]).await.unwrap();
        assert_eq!(result, b"test");
    }
}
