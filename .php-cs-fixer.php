<?php

// Exclude file inside external libs
$finder = PhpCsFixer\Finder::create()
    ->exclude('vendor')
    ->exclude('externalLib')
    ->in(__DIR__)
    ;
$config = new PhpCsFixer\Config();
$config
    ->setRules([
        '@PSR12' => true,
        '@Symfony' => true,
        'phpdoc_annotation_without_dot' => false,
        'phpdoc_summary' => false,
        'single_quote' => true,
        'ordered_imports' => true,
        'no_break_comment' => false,
        'blank_line_before_statement' => false,
        'increment_style' => false,
        'yoda_style' => false,
        'phpdoc_to_comment' => false, // Need to disable this to use single-line suppressions with Psalm
        'array_syntax' => ['syntax' => 'short'],
        'no_useless_else' => true,
        'single_line_throw' => false,
        'heredoc_to_nowdoc' => true,
        'global_namespace_import' => true,
        'fully_qualified_strict_types' => true,
    ])
    ->setUsingCache(true)
    ->setFinder($finder);

return $config;
