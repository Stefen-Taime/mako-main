# WASM Plugin Transform Examples

Pipelines that use WebAssembly plugins for custom transformation logic.

## Pipelines

| Pipeline | Language | Description |
|----------|----------|-------------|
| [pipeline-beer-wasm-go.yaml](pipeline-beer-wasm-go.yaml) | Go (TinyGo) | Enrich beer data with Go WASM plugin |
| [pipeline-beer-wasm-rust.yaml](pipeline-beer-wasm-rust.yaml) | Rust | Enrich beer data with Rust WASM plugin |

## Plugin Source Code

| File | Language | Description |
|------|----------|-------------|
| [main.go](main.go) | Go | TinyGo WASM plugin source |
| [Cargo.toml](Cargo.toml) + [src/lib.rs](src/lib.rs) | Rust | Rust WASM plugin source |
| [plugin-go.wasm](plugin-go.wasm) | - | Pre-compiled Go WASM binary |
| [plugin-rust.wasm](plugin-rust.wasm) | - | Pre-compiled Rust WASM binary |

## How It Works

WASM plugins receive a JSON event as input and return a transformed JSON event. The plugin exposes a `transform` function:

```yaml
transforms:
  - name: enrich
    type: plugin
    config:
      path: ./plugin.wasm
      # function: transform    # default export name
```

## Building Plugins

**Go (TinyGo):**
```bash
tinygo build -o plugin.wasm -target=wasi main.go
```

**Rust:**
```bash
cargo build --target wasm32-wasip1 --release
cp target/wasm32-wasip1/release/plugin.wasm .
```

## Run

```bash
mako validate examples/transforms/wasm/pipeline-beer-wasm-go.yaml
mako run examples/transforms/wasm/pipeline-beer-wasm-go.yaml
```
