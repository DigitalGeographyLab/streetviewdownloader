# streetviewdownloader

**Streetviewdownloader** is a Python library and utility to download images from Google’s StreetView Static API.

## Installation

**Streetviewdownloader** is available from PyPi:

```
pip install streetviewdownloader
```

Alternatively, it can be built from source:

```
git clone https://gitlab.com/christoph.fink/streetviewdownloader
cd streetviewdownloader
pip install .
```

## Usage

### Command line tool

```
streetviewdownloader --api-key YOUR_API_KEY --url-signing-key YOUR_URL_SIGNING_KEY --extent "POLYGON((24.9791 60.2021, 24.9609 60.2002, 24.9642 60.1894, 24.9826 60.1867, 24.9879 60.1950, 24.9791 60.2021))" --output-directory "Hermanni"
```

All command line options can alternatively be specified in a configuration file saved to `${XDG_CONFIG_HOME}/streetviewdownloader.yaml` or `%APPDATA%\streetviewdownloader.yaml`, depending on your operating system.

### Library

Streetviewdownloader can also be used via its Python API. Find examples and API reference at https://streetviewdownloader.readthedocs.io
