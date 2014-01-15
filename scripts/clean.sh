#!/bin/bash

find ../smallest/* -maxdepth 0 -type d -exec rm -rf '{}' ';'

