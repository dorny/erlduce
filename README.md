**WARNING - Student research project in very active (deadline is comming) development**

# Erlduce

Experimental MapReduce framework written in Erlang.

## Instalation

### Prerequisites:

* Erlang R15B01 or newer
* gcc
* git
* ssh access between all nodes
* paswordless ssh from master to all nodes


### Donwload and compile

```
git clone https://github.com/dorny/erlduce.git
cd erlduce
./rebar compile
```

### Configure

1. Copy and edit `erlduce-sample.config` to `erlduce.config`.
1. Add `bin` folder to your `PATH`
1. `erlduce-dist` - will distribute your compiled sources and configs to all nodes in cluster


## Usage

Run `erlduce help` or `erlduce <command> -help` to show usage for erlduce.

Run `edfs help` or `edfs <command> -help` to show usage for edfs.

## How it Works

TODO

## License

MIT
