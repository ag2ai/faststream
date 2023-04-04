#!/bin/bash

# exit when any command fails
set -e

echo "Cleanup existing build artifacts"
rm -rf docusaurus/docs

echo "Run nbdev_mkdocs docs"
nbdev_mkdocs docs

echo "Copy newly generated markdown files to docusaurus directory"
cp -r mkdocs/docs docusaurus/

echo "Generate API docs"
python3 -c "from fastkafka._docusaurus_helper import fix_invalid_syntax_in_markdown, generate_markdown_docs; fix_invalid_syntax_in_markdown('./docusaurus/docs'); generate_markdown_docs('fastkafka', './docusaurus/docs')"

echo "Run docusaurus build"
cd docusaurus && npm run build

