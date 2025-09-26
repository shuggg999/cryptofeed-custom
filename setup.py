#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Cryptofeed Setup Configuration
Build Cython extensions for performance-critical components
"""
import os
import sys
from setuptools import setup, Extension
from Cython.Build import cythonize

# Environment variable to control assertion compilation
CYTHON_WITHOUT_ASSERTIONS = os.environ.get('CYTHON_WITHOUT_ASSERTIONS', True)

# Define compiler directives
compiler_directives = {
    'boundscheck': False,
    'wraparound': False,
    'cdivision': True,
    'embedsignature': True,
    'language_level': 3,
}

# Define macros based on assertions setting
define_macros = []
if CYTHON_WITHOUT_ASSERTIONS:
    define_macros.append(('CYTHON_WITHOUT_ASSERTIONS', '1'))

# Extensions to build
extensions = [
    Extension(
        "cryptofeed.types",
        sources=["cryptofeed/types.pyx"],
        define_macros=define_macros,
        extra_compile_args=["-O3"],
    ),
]

# Cythonize extensions
try:
    extensions = cythonize(
        extensions,
        compiler_directives=compiler_directives,
        annotate=False,  # Set to True for debugging
    )
except ImportError:
    print("Cython not available, skipping extension compilation")
    extensions = []

setup(
    name="cryptofeed",
    version="2.4.1",
    description="Cryptocurrency exchange data feed handler",
    author="Bryant Moscon",
    author_email="bmoscon@gmail.com",
    url="https://github.com/bmoscon/cryptofeed",
    packages=["cryptofeed"],
    ext_modules=extensions,
    python_requires=">=3.9",
    zip_safe=False,
    install_requires=[
        "aiohttp>=3.11.6",
        "websockets>=14.1",
        "pyyaml",
        "yapic.json>=1.6.3",
        "order_book>=0.6.0",
        "aiofile>=2.0.0",
        "requests>=2.18.4",
    ],
    extras_require={
        "redis": ["redis>=4.0.0"],
        "arctic": ["arctic"],
        "kafka": ["aiokafka>=0.6.0"],
        "mongo": ["motor"],
        "postgres": ["asyncpg"],
        "zmq": ["pyzmq"],
        "rabbit": ["aio-pika"],
        "gcp": ["google-cloud-pubsub>=2.0.0"],
        "azure": ["azure-servicebus>=7.0.0"],
        "all": [
            "redis>=4.0.0",
            "arctic",
            "aiokafka>=0.6.0",
            "motor",
            "asyncpg",
            "pyzmq",
            "aio-pika",
            "google-cloud-pubsub>=2.0.0",
            "azure-servicebus>=7.0.0"
        ]
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: XFree86 1.1 License",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3 :: Only",
        "Topic :: Office/Business :: Financial :: Investment",
        "Topic :: Software Development :: Libraries",
        "Topic :: System :: Networking",
        "Operating System :: OS Independent",
    ]
)