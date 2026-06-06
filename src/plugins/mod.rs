use std::fmt;

use async_trait::async_trait;

#[derive(Debug, Clone)]
pub enum PluginError {
    ExecutionFailed(String),
    AuthenticationFailed(String),
    PluginNotFound(String),
    InvalidInput(String),
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PluginError::ExecutionFailed(msg) => write!(f, "execution failed: {}", msg),
            PluginError::AuthenticationFailed(msg) => write!(f, "authentication failed: {}", msg),
            PluginError::PluginNotFound(name) => write!(f, "plugin not found: {}", name),
            PluginError::InvalidInput(msg) => write!(f, "invalid input: {}", msg),
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

    fn on_command(&self, _command: &str, _args: &[Vec<u8>]) -> Option<Vec<u8>> {
        None
    }
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
        self.plugins.push(plugin);
    }

    pub fn register_auth_plugin(&mut self, plugin: Box<dyn AuthPlugin>) {
        self.auth_plugins.push(plugin);
    }

    pub fn get_plugin(&self, name: &str) -> Option<&dyn Plugin> {
        self.plugins.iter().find(|p| p.name() == name).map(|p| &**p)
    }

    pub fn get_auth_plugin(&self, name: &str) -> Option<&dyn AuthPlugin> {
        self.auth_plugins
            .iter()
            .find(|p| p.name() == name)
            .map(|p| &**p)
    }

    pub async fn execute_custom_command(
        &self,
        command: &str,
        args: &[Vec<u8>],
    ) -> Result<Vec<u8>, PluginError> {
        for plugin in &self.plugins {
            if let Some(result) = plugin.on_command(command, args) {
                return Ok(result);
            }
            match plugin.execute(command, args).await {
                Ok(output) => return Ok(output),
                Err(PluginError::ExecutionFailed(_)) => continue,
                Err(e) => return Err(e),
            }
        }
        Err(PluginError::PluginNotFound(format!(
            "no plugin handles command: {}",
            command
        )))
    }

    pub async fn check_auth(&self, credentials: &[u8]) -> bool {
        if self.auth_plugins.is_empty() {
            return true;
        }
        for plugin in &self.auth_plugins {
            match plugin.authenticate(credentials).await {
                Ok(true) => return true,
                Ok(false) => continue,
                Err(_) => continue,
            }
        }
        false
    }

    pub fn notify_connect(&self, client_id: u64) {
        for plugin in &self.plugins {
            plugin.on_connect(client_id);
        }
    }

    pub fn notify_disconnect(&self, client_id: u64) {
        for plugin in &self.plugins {
            plugin.on_disconnect(client_id);
        }
    }

    pub fn plugin_count(&self) -> usize {
        self.plugins.len()
    }

    pub fn auth_plugin_count(&self) -> usize {
        self.auth_plugins.len()
    }
}

pub struct EchoPlugin;

#[async_trait]
impl Plugin for EchoPlugin {
    fn name(&self) -> &str {
        "echo"
    }

    fn version(&self) -> &str {
        "1.0.0"
    }

    async fn execute(&self, command: &str, args: &[Vec<u8>]) -> Result<Vec<u8>, PluginError> {
        match command {
            "echo" => {
                let mut output = Vec::new();
                for (i, arg) in args.iter().enumerate() {
                    if i > 0 {
                        output.push(b' ');
                    }
                    output.extend_from_slice(arg);
                }
                Ok(output)
            }
            "reverse" => {
                let input = args.first().cloned().unwrap_or_default();
                let reversed: Vec<u8> = input.into_iter().rev().collect();
                Ok(reversed)
            }
            "uppercase" => {
                let input = args.first().cloned().unwrap_or_default();
                Ok(input.to_ascii_uppercase())
            }
            _ => Err(PluginError::ExecutionFailed(format!(
                "unknown command: {}",
                command
            ))),
        }
    }
}

pub struct TokenAuthPlugin {
    valid_tokens: std::collections::HashSet<String>,
}

impl TokenAuthPlugin {
    pub fn new(tokens: Vec<String>) -> Self {
        Self {
            valid_tokens: tokens.into_iter().collect(),
        }
    }
}

#[async_trait]
impl AuthPlugin for TokenAuthPlugin {
    fn name(&self) -> &str {
        "token_auth"
    }

    async fn authenticate(&self, credentials: &[u8]) -> Result<bool, PluginError> {
        let token = std::str::from_utf8(credentials)
            .map_err(|e| PluginError::AuthenticationFailed(e.to_string()))?;
        Ok(self.valid_tokens.contains(token))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn echo_plugin_echo() {
        let plugin = EchoPlugin;
        let result = plugin
            .execute("echo", &[b"hello".to_vec(), b"world".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, b"hello world");
    }

    #[tokio::test]
    async fn echo_plugin_reverse() {
        let plugin = EchoPlugin;
        let result = plugin
            .execute("reverse", &[b"abc".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, b"cba");
    }

    #[tokio::test]
    async fn echo_plugin_uppercase() {
        let plugin = EchoPlugin;
        let result = plugin
            .execute("uppercase", &[b"hello".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, b"HELLO");
    }

    #[tokio::test]
    async fn echo_plugin_unknown_command() {
        let plugin = EchoPlugin;
        let err = plugin.execute("nope", &[]).await.unwrap_err();
        match err {
            PluginError::ExecutionFailed(msg) => assert!(msg.contains("unknown command")),
            _ => panic!("expected ExecutionFailed"),
        }
    }

    #[tokio::test]
    async fn plugin_manager_execute() {
        let mut mgr = PluginManager::new();
        mgr.register_plugin(Box::new(EchoPlugin));

        let result = mgr
            .execute_custom_command("echo", &[b"test".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, b"test");
    }

    #[tokio::test]
    async fn plugin_manager_not_found() {
        let mgr = PluginManager::new();
        let err = mgr.execute_custom_command("anything", &[]).await.unwrap_err();
        match err {
            PluginError::PluginNotFound(_) => {}
            _ => panic!("expected PluginNotFound"),
        }
    }

    #[tokio::test]
    async fn token_auth_valid() {
        let plugin = TokenAuthPlugin::new(vec!["secret123".into()]);
        assert!(plugin.authenticate(b"secret123").await.unwrap());
    }

    #[tokio::test]
    async fn token_auth_invalid() {
        let plugin = TokenAuthPlugin::new(vec!["secret123".into()]);
        assert!(!plugin.authenticate(b"wrong").await.unwrap());
    }

    #[tokio::test]
    async fn check_auth_with_no_plugins() {
        let mgr = PluginManager::new();
        assert!(mgr.check_auth(b"anything").await);
    }

    #[tokio::test]
    async fn check_auth_with_valid_token() {
        let mut mgr = PluginManager::new();
        mgr.register_auth_plugin(Box::new(TokenAuthPlugin::new(vec!["abc".into()])));
        assert!(mgr.check_auth(b"abc").await);
    }

    #[tokio::test]
    async fn check_auth_with_invalid_token() {
        let mut mgr = PluginManager::new();
        mgr.register_auth_plugin(Box::new(TokenAuthPlugin::new(vec!["abc".into()])));
        assert!(!mgr.check_auth(b"wrong").await);
    }

    #[tokio::test]
    async fn plugin_lifecycle_hooks() {
        let mut mgr = PluginManager::new();
        mgr.register_plugin(Box::new(EchoPlugin));

        mgr.notify_connect(1);
        mgr.notify_disconnect(1);
    }

    #[test]
    fn get_plugin() {
        let mut mgr = PluginManager::new();
        mgr.register_plugin(Box::new(EchoPlugin));

        assert!(mgr.get_plugin("echo").is_some());
        assert!(mgr.get_plugin("nope").is_none());
        assert_eq!(mgr.plugin_count(), 1);
    }
}
