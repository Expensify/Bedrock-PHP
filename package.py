#!/usr/bin/env python3

import re


def bumpComposerVersion(majorVersion, minorVersion, patchVersion):
    newMajorVersion = majorVersion + 1 if minorVersion == 99 and patchVersion == 99 else majorVersion

    if minorVersion == 99 and patchVersion == 99:
        newMinorVersion = 0
    elif patchVersion == 99:
        newMinorVersion = minorVersion + 1
    else:
        newMinorVersion = minorVersion

    newPatchVersion = 0 if patchVersion == 99 else patchVersion + 1

    return f'{newMajorVersion}.{newMinorVersion}.{newPatchVersion}'


def run():
    with open('./composer.json', 'r+') as composerJSON:
        output = ''
        currentVersion = ''
        for line in composerJSON:
            newLine = line
            currentVersion = re.search('"version": "((\d+)\.(\d+)\.(\d+))"', line)
            if currentVersion:
                majorVersion = int(currentVersion.group(2))
                minorVersion = int(currentVersion.group(3))
                patchVersion = int(currentVersion.group(4))

                newVersion = bumpComposerVersion(majorVersion, minorVersion, patchVersion)

                print(f'Previous version: {currentVersion.group(1)}')
                print(f'New version: {newVersion}')
                newLine = f'    "version": "{newVersion}",\n'

            output += newLine

        composerJSON.seek(0)
        composerJSON.write(output)
        composerJSON.truncate()


if __name__ == '__main__':
    run()
