<?php

// Exclude file inside external libs
$finder = Symfony\CS\Finder\DefaultFinder::create()
    ->exclude('vendor')
    ->exclude('externalLib')
    ->in(__DIR__)
        ;

return Symfony\CS\Config\Config::create()
       ->fixers([
            'short_array_syntax',
            '-unalign_double_arrow',
            '-unalign_equals',
            '-single_quote',
            '-pre_increment',
            '-phpdoc_annotation_without_dot',
            '-phpdoc_short_description'
        ])
       ->setUsingCache(true)
       ->finder($finder)
       ;
