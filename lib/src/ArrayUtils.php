<?php

namespace Expensify\Libs;

/**
 * Utility functions for arrays.
 */
class ArrayUtils
{
    /**
     * Check if an array is associative.
     *
     * @param $ar   array
     *
     * @return bool
     */
    public static function isAssociative($ar)
    {
        return is_array($ar) && array_keys($ar) !== range(0, count($ar) - 1);
    }

    /**
     * Returns whether two multi-dimensional arrays are equal.
     * For non-array values, we use a simple ==.
     * For array values, we call this method recursively.
     *
     * @param array $array1
     * @param array $array2
     *
     * @return bool
     */
    public static function arraysAreEqual($array1, $array2)
    {
        if (array_keys($array1) !== array_keys($array2)) {
            return false;
        }

        foreach ($array1 as $key => $value) {
            // If one value from $arrayX, compare with the corresponding value in $arrayY
            if (is_array($array1[$key]) || is_array($array2[$key])) {
                // If one of the two is not an array, at least one value differs
                if (!is_array($array2[$key]) || !is_array($array2[$key])) {
                    return false;
                }

                // Call arrays_equal recursively on the two array values
                if (!self::arraysAreEqual($array1[$key], $array2[$key])) {
                    return false;
                }
            }

            // The value is not an array, use simple equals
            else {
                if ($array1[$key] != $array2[$key]) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Find one item in an array.
     *
     * @param array    $list
     * @param callable $filter A callable iterator that returns true if found
     *
     * @return mixed|null the object if found, null if not found
     */
    public static function find(array $list, callable $filter)
    {
        $matchingItems = array_values(array_filter($list, $filter));

        return empty($matchingItems) ? null : $matchingItems[0];
    }

    /**
     * Find an index of one item in an array.
     *
     * @param array    $list
     * @param callable $filter A callable iterator that returns true iff found
     *
     * @return mixed|null the object if found, null if not found
     */
    public static function findIndex(array $list, callable $filter)
    {
        $matchingItems = array_filter($list, $filter);

        return empty($matchingItems) ? null : array_keys($matchingItems)[0];
    }

    /**
     * Returns whether the callable returns true for at least one element in the given array.
     *
     * @param array    $list
     * @param callable $filter
     *
     * @return bool
     */
    public static function some(array $list, callable $filter)
    {
        foreach ($list as $index => $elem) {
            if ($filter($elem, $index)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Returns whether the callable returns true for all the elements in the given array.
     *
     * @param array    $list
     * @param callable $filter
     *
     * @return bool
     */
    public static function all(array $list, callable $filter)
    {
        foreach ($list as $elem) {
            if (!$filter($elem)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Test if the provided index path exist in an array.
     *
     * @param array            $array
     * @param array|int|string $index
     *
     * @return bool
     */
    public static function keyExists($array, $index)
    {
        if (!is_array($array)) {
            return false;
        }

        if (is_string($index) || is_numeric($index)) {
            return array_key_exists($index, $array);
        }

        if (is_array($index)) {
            if (count($index) === 1) {
                return array_key_exists($index[0], $array);
            } else {
                return self::keyExists(self::get($array, $index[0]), array_slice($index, 1));
            }
        }

        return false;
    }

    /**
     * Check if multiple keys exists in an array.
     *
     * @param array $arr
     * @param array $indexes
     *
     * @return bool
     */
    public static function keysExists(array $arr, array $indexes)
    {
        if (count($indexes) === 0) {
            return false;
        }
        foreach ($indexes as $index) {
            if (!self::keyExists($arr, $index)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Set the value in an array, at the correct position.
     *
     * @param array $array
     * @param array $index
     * @param mixed $value
     *
     * @return mixed
     */
    public static function set(array &$array, array $index, $value)
    {
        if (!count($index)) {
            return;
        }

        if (!isset($array[$index[0]])) {
            $array[$index[0]] = [];
        }

        if (count($index) === 1) {
            return $array[$index[0]] = $value;
        }

        return self::set($array[$index[0]], array_slice($index, 1), $value);
    }

    /**
     * Returns the element in the given index of the array or the default
     * if the index isn't set
     * e.g.: ArrayUtils::get( ['carlos' => 'c', 'thomas' => 't'], 'carlos' ) === 'c'
     *       ArrayUtils::get( ['carlos' => 'c', 'thomas' => 't'], 'michael' ) === null
     *       ArrayUtils::get( ['carlos' => 'c', 'thomas' => 't'], 'michael', '' ) === ''
     *       ArrayUtils::get( ['a' => ['a1' => 1] ] , ['a','a1'] ) === 1.
     *
     * @param mixed            $array
     * @param string|int|array $index
     * @param mixed            $default (optional)
     *
     * @return mixed
     */
    public static function get($array, $index, $default = null)
    {
        if (!is_array($array)) {
            return $default;
        }

        if (is_array($index)) {
            foreach ($index as $step) {
                if (!isset($array[$step])) {
                    return $default;
                }
                $array = $array[$step];
            }

            return $array;
        }

        if (!is_string($index) && !is_int($index)) {
            return $default;
        }

        if (!isset($array[$index])) {
            return $default;
        }

        return $array[$index];
    }

    /**
     * Gets an array of values from an array of associative arrays based on the key.
     * Returns $default if the key isn't set.
     *
     * eg:
     * $arr = [ 0 => [ 'id' => '1', 'abc' => '123' ], 1 => [ 'id' => '2', 'abc' => '456' ] ];
     * ArrayUtils::pluck($arr, 'abc') === [ 0 => '123', 1 => '456' ]
     *
     * @param       $inputArray
     * @param       $key
     * @param mixed $default    (optional)
     *
     * @return mixed
     */
    public static function pluck($inputArray, $key, $default = null)
    {
        $propertyValues = [];
        foreach ($inputArray as $inputValue) {
            if (!isset($inputValue[$key])) {
                return $default;
            }

            $propertyValues[] = $inputValue[$key];
        }

        return $propertyValues;
    }

    /**
     * Same as array_map but with inverse param order.
     *
     * @param array    $list
     * @param callable $transform
     *
     * @return array
     */
    public static function map(array $list, callable $transform)
    {
        return array_map($transform, $list);
    }

    /**
     * Returns the first element of $list, whose $property matches the $value.
     *
     * @param array[] $list
     * @param string  $property
     * @param mixed   $value
     *
     * @return mixed|null
     */
    public static function findBy(array $list, $property, $value)
    {
        foreach ($list as $item) {
            if (self::get($item, $property) === $value) {
                return $item;
            }
        }

        return;
    }

    /**
     * Returns all the elements of $list, whose $property matches the $value.
     *
     * @param array[]          $list
     * @param string|int|array $property
     * @param mixed            $value
     *
     * @return array
     */
    public static function filterBy(array $list, $property, $value)
    {
        // array_filter preserves keys, so we call array_values to get a zero indexed array
        return array_values(array_filter($list, function ($item) use ($property, $value) {
            return self::get($item, $property) === $value;
        }));
    }

    /**
     * Given an array, filters elements on which the function returns false.
     *
     * @param array    $list
     * @param callable $function
     *
     * @return array
     */
    public static function filter(array $list, callable $function)
    {
        return array_values(array_filter($list, $function));
    }

    /**
     * Remove some keys from an array, if they exists.
     *
     * @todo Add support for "nested key", aka ['a]['b]
     * @todo Add option to clone the input instead of modifying it directly
     *
     * @param array $input
     * @param mixed $excludeList
     * @param bool  $recursive
     */
    public static function omit(array &$input, $excludeList, $recursive = false)
    {
        if ((is_numeric($excludeList) || is_string($excludeList)) &&
            isset($input[$excludeList])
        ) {
            unset($input[$excludeList]);
        } elseif (is_array($excludeList)) {
            foreach ($excludeList as $excludeKey) {
                if ((is_numeric($excludeKey) || is_string($excludeKey)) &&
                    isset($input[$excludeKey])
                ) {
                    unset($input[$excludeKey]);
                }
            }
        } else {
            // Exclude list is an invalid type, return immediately
            return;
        }

        if ($recursive) {
            // Recurse into each  array value
            foreach ($input as &$value) {
                if (is_array($value)) {
                    self::omit($value, $excludeList, true);
                }
            }
        }
    }

    /**
     * Searches for $needle in the multidimensional array $haystack.
     *
     * @param mixed $needle   The item to search for
     * @param array $haystack The array to search
     *
     * @return array|null The first indice of $needle in $haystack across the
     *                    various dimensions. null if $needle was not found.
     */
    public static function recursiveSearch($needle, $haystack)
    {
        foreach ($haystack as $key => $value) {
            if ($needle === $value) {
                return [$key];
            } elseif (is_array($value) && $subkey = self::recursiveSearch($needle, $value)) {
                array_unshift($subkey, $key);

                return $subkey;
            }
        }
    }

    /**
     * Removes all falsey values from an array.
     *
     * @param array $input
     *
     * @return array
     */
    public static function compact(array $input)
    {
        $result = [];
        foreach ($input as $val) {
            if ($val) {
                $result[] = $val;
            }
        }

        return $result;
    }

    /**
     * Returns the input. If input is an object it returns it as an associative array instead.
     * Also works with multidimensional objects.
     *
     * @param mixed $data
     *
     * @return mixed
     */
    public static function objectToArray($data)
    {
        if (is_array($data) || is_object($data)) {
            $result = [];
            foreach ($data as $key => $value) {
                $result[$key] = self::objectToArray($value);
            }

            return $result;
        }

        return $data;
    }

    /**
     * Does a deep alphabetical sort of an array.
     *
     * @param array $array
     *
     * @return array
     */
    public static function deepSortByKey(array &$array)
    {
        if (empty($array)) {
            return [];
        }

        foreach ($array as &$value) {
            if (is_array($value)) {
                self::deepSortByKey($value);
            }
        }

        return ksort($array);
    }
}
