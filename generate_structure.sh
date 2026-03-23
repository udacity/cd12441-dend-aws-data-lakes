#!/bin/bash
grep '/$' structure.txt | while IFS= read -r dir; do mkdir -p "$dir"; done
grep -v '/$' structure.txt | while IFS= read -r file; do mkdir -p "$(dirname "$file")" && touch "$file"; done
