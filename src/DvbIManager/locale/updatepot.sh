#!/bin/sh
# SPDX-License-Identifier: GPL-2.0-or-later
set -eu
cd "$(dirname "$0")"
temporary=$(mktemp -d)
trap 'rm -rf "$temporary"' EXIT HUP INT TERM

version=$(sed -n 's/^__version__ = "\([^"]*\)"/\1/p' ../__init__.py)
xgettext --language=Python --keyword=_ --keyword=pgettext:1c,2 --from-code=UTF-8 \
    --no-wrap --sort-output --add-comments=TRANSLATORS: \
    --package-name="DVB-I Manager" --package-version="$version" \
    --msgid-bugs-address="https://github.com/oe-alliance-plugins/DvbIManager/issues" \
    -o "$temporary/python.pot" ../__init__.py ../plugin.py ../DvbI.py
sed -i 's/charset=CHARSET/charset=UTF-8/' "$temporary/python.pot"
python3 xml2po.py ../setup.xml > "$temporary/xml.pot"
msgcat --use-first --sort-output --no-wrap -o "$temporary/DvbIManager.pot" "$temporary/python.pot" "$temporary/xml.pot"

# Do not change the POT timestamp when all extracted messages are unchanged.
if [ -f DvbIManager.pot ]; then
    sed '/^"POT-Creation-Date:/d' DvbIManager.pot > "$temporary/old.pot"
    sed '/^"POT-Creation-Date:/d' "$temporary/DvbIManager.pot" > "$temporary/new.pot"
    if ! cmp -s "$temporary/old.pot" "$temporary/new.pot"; then
        cp "$temporary/DvbIManager.pot" DvbIManager.pot
    fi
else
    cp "$temporary/DvbIManager.pot" DvbIManager.pot
fi
for language in ./*.po; do
    [ -f "$language" ] || continue
    msgmerge --update --backup=none --no-fuzzy-matching --no-wrap "$language" DvbIManager.pot
    msgfmt --check --check-format -o /dev/null "$language"
done
