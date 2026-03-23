#!/bin/bash
find . -type d -not -path '*/.*' -not -path '*/node_modules' -not -path '*/__pycache__' | sed 's|$|/|' > structure.txt
find . -type f -not -path '*/.*' -not -path '*/node_modules/*' -not -path '*/__pycache__/*' >> structure.txt
