<?php

// Exclude file inside external libs
$finder = PhpCsFixer\Finder::create()
    ->exclude('vendor')
    ->exclude('externalLib')
    ->in(__DIR__)
;
return PhpCsFixer\Config::create()
    ->setRules([
        '@PSR2' => true,
        '@Symfony' => true,
        'phpdoc_annotation_without_dot' => false,
        'phpdoc_summary' => false,
        'pre_increment' => false,
        'single_quote' => false,
        'ordered_imports' => true,
        'no_break_comment' => false,
        'binary_operator_spaces' => ['align_double_arrow' => null, 'align_equals' => null],
        'blank_line_before_statement' => false,
        'increment_style' => null,
        'yoda_style' => null,
        'phpdoc_to_comment' => null, // Need to disable this to use single-line suppressions with Psalm
        'array_syntax' => ['syntax' => 'short'],
    ])
    ->setUsingCache(true)
    ->setFinder($finder)
    ;
