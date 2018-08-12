# opentrade
OpenTrade is an open source OEMS, and algorithmic trading platform, designed for simplicity, flexibility and performance. 

[**Demo**](http://demo.opentradesolutions.com)

# Features:
* Strictly follows [Google C++ Style Guild](https://google.github.io/styleguide/cppguide.html)
* Multi-level account functionality
* Super simple API interfaces for market data adapter, exchange connectivity and execution/alpha algo
* Multi-source market data support, e.g., different FX pricing sources
* Pre-trade risk limits on multi-level accounts
* Post-trade risk integrated with [OpenRisk](https://github.com/opentradesolutions/openrisk)
* [Multi-theme web frontend](http://demo.opentradesolutions.com)
* Fully thread-safe design, everything can be modified during runtime, e.g., reload symbol list, modify tick size table, lot-size, exchange timezone and trading/break period etc.
* Built-in execution simulator
* Simple configuration

# Steps to run on Ubuntu 18.04
* **Compile**
  * Prepare dev environment.
  ```bash
  sudo apt-get update \
  && apt-get install -y \
    g++  \
    make \
    cmake \
    clang-format \
    clang \
    python \
    python-dev \
    vim \
    exuberant-ctags \
    git \
    wget \
    libssl-dev \
    libboost-program-options-dev \
    libboost-system-dev \
    libboost-date-time-dev \
    libboost-filesystem-dev \
    libboost-iostreams-dev \
    libsoci-dev \
    libpq-dev \
    libquickfix-dev \
    libtbb-dev \
    liblog4cxx-dev
  ```
  * Build
  ```bash
  git clone https://github.com/opentradesolutions/opentrade
  cd opentrade
  make debug
  ```
  
  * **Setup database**
    ```bash
    ```
