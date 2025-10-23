# Bedrock-PHP
This is a library to interact with [Bedrock](https://github.com/Expensify/Bedrock)

# Publishing Your Changes
Versions are automatically created when a pull request is merged to `main`. The workflow will:

1. Automatically increment the patch version in `composer.json`
1. Create a PR with the version bump
1. Auto-approve and merge the version bump PR
1. Tag the new version
1. Comment on the original PR with the release version

No manual intervention is required - just merge your PR and the rest happens automatically!
