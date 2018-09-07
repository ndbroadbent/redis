#!/bin/bash
set -e

echo "Renaming files..."
find . -name "*slave*" \
  -not -path "./.git/*" \
  -not -path "./utils/rename_master_slave.sh" \
  -exec bash -c 'mv "$0" "`echo \"$0\" | sed s/slave/replica/`"' {} \;

for REPLACEMENT in \
  MASTERS/PRIMARIES \
  MASTER/PRIMARY \
  Masters/Primaries \
  Master/Primary \
  masters/primaries \
  master/primary \
  SLAVE/REPLICA \
  Slave/Replica \
  slave/replica \
; do
  echo "Replacing ${REPLACEMENT%/*} with ${REPLACEMENT#*/}..."
  git grep --files-with-matches "${REPLACEMENT%/*}" \
    -- './*' ':!*rename_master_slave.sh' | \
    xargs --no-run-if-empty sed -i -e "s/$REPLACEMENT/g"
done

echo "Fixing bad replacements..."
# Fix things like GitHub URLs
for REPLACEMENT in \
  '/primary/%/master/' \
; do
  echo "Replacing ${REPLACEMENT%\%*} with ${REPLACEMENT#*\%}..."
  git grep --files-with-matches "${REPLACEMENT%\%*}" \
    -- './*' ':!*rename_master_slave.sh' | \
    xargs --no-run-if-empty sed --in-place --expression "s%$REPLACEMENT%g"
done
