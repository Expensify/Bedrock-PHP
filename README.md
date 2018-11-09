# Bedrock-PHP
This is a library to interact with [Bedrock](https://github.com/Expensify/Bedrock)

# Development
1. Create a new branch
1. Make changes and commit as normal
1. Push your branch up to GH
1. To test in Web-Expensify or Web-Secure, grab your most recent commit hash that is pushed up
    1. You can find this via `git log` or by looking on GH
    1. Copy the full commit hash
    1. Update `composer.json` to have `dev-[your branch name]#[commit hash]`
    1. Run `composer update expensify/bedrock-php` inside the VM 


## Bumping Version in Composer
1. Look at your PR and find the commit that merged your code into `master` ![image](https://user-images.githubusercontent.com/4073354/47167869-b197cb80-d2bc-11e8-9406-0cb58774570c.png)
1. Click the commit hash which will take you to a page like this: ![image](https://user-images.githubusercontent.com/4073354/47167935-dab85c00-d2bc-11e8-88f0-fb780aa97c53.png) copy the full commit hash from the URL
1. Change the commit hash in Web-Expenssify composer.json (and Web-Secure if needed): ![image](https://user-images.githubusercontent.com/4073354/47168070-3b479900-d2bd-11e8-8381-d546cf34b11b.png)
1. Update the composer in your VM: `Expensidev/scripts/composer.sh update expensify/bedrock-php` or `➜  Web-Expensify vssh composer update expensify/bedrock-php`
1. Commit changes to composer.json and composer.lock and create PR.
