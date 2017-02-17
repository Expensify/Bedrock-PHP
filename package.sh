#!/bin/bash

echo Current version:
sed -n -e '/"version"/p' composer.json

echo What is the new version?
read targetVersion

sed -i -e 's/"version": "[0-9]*\.[0-9]*\.[0-9]*",/"version": "'$targetVersion'",/g' composer.json

echo Committing build
git add .
git commit -m "build version $targetVersion"

echo Tagging release...
git tag -a "$targetVersion" -m "$targetVersion"

echo Pushing tag
git push origin --tags