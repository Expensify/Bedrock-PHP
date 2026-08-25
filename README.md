# Bedrock-PHP
This is a library to interact with [Bedrock](https://github.com/Expensify/Bedrock)

# Publishing Your Changes
Merging a pull request into `main` publishes a new version. A workflow tags the
merge commit with the next patch version and comments that version on the pull
request. There is nothing to tag by hand.

`composer.json` has no `version` field on purpose. Composer resolves this
library to the version declared there in preference to the tag name, so the tag
is the only place a version is recorded.

To pick up a new version, update the `expensify/bedrock-php` constraint in the
consuming repository, such as Web-Expensify or Web-Secure, and run
`composer update expensify/bedrock-php`.
