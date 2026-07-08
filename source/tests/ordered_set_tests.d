module tests.ordered_set_tests;

version (unittest)
{
    import std.stdio;
    import dreads.containers;
    import unit_threaded;

    @("OrderedSet.add_elements")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.toString().shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.add_duplicates")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(1);
        a.add(2);
        a.add(3);
        a.toString.shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.length")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.length().shouldEqual(3);
    }

    @("OrderedSet.remove_elements")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.remove(1);
        a.toString().shouldEqual("{2}");
    }

    @("OrderedSet.has_element")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.has(1).shouldBeTrue();
        a.has(3).shouldBeFalse();
    }

    @("OrderedSet.in_operator")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        (1 in a).shouldBeTrue();
        (3 in a).shouldBeFalse();
    }

    @("OrderedSet.equality")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(1);
        (a == b).shouldBeTrue();
    }

    @("OrderedSet.union_operation")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(3);
        auto result = a + b;
        result.toString().shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.difference_operation")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(3);
        auto result = a - b;
        result.toString().shouldEqual("{1}");
    }

    @("OrderedSet.intersection_operation")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(3);
        auto result = a.intersection(b);
        result.toString().shouldEqual("{2}");
    }

    @("OrderedSet.symmetric_difference")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(3);
        auto result = a.symmetricDifference(b);
        result.toString().shouldEqual("{1, 3}");
    }

    @("OrderedSet.is_subset")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(1);
        b.add(2);
        a.isSubsetOf(b).shouldBeTrue();
    }

    @("OrderedSet.is_superset")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(1);
        b.add(2);
        a.isSuperSetOf(b).shouldBeTrue();
    }

    @("OrderedSet.union_with_empty")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        auto result = a + b;
        result.toString().shouldEqual("{}");
    }

    @("OrderedSet.intersection_with_empty")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        auto result = a.intersection(b);
        result.toString().shouldEqual("{}");
    }

    @("OrderedSet.difference_with_empty")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        auto result = a - b;
        result.toString().shouldEqual("{}");
    }

    @("OrderedSet.add_negative_elements")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(-1);
        a.add(-2);
        a.toString().shouldEqual("{-2, -1}");
    }

    @("OrderedSet.add_large_elements")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1000000);
        a.add(5000000);
        a.toString().shouldEqual("{1000000, 5000000}");
    }

    @("OrderedSet.empty_subset_check")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.isSubsetOf(b).shouldBeTrue();
    }

    @("OrderedSet.empty_superset_check")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        b.isSuperSetOf(a).shouldBeTrue();
    }

    @("OrderedSet.empty_equality")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        (a == b).shouldBeTrue();
    }

    @("OrderedSet.add_remove_duplicates")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(1);
        a.add(2);
        a.remove(1);
        a.toString().shouldEqual("{2}");
    }

    @("OrderedSet.add_string_elements")
    @safe unittest
    {
        OrderedSet!string a;
        a.add("foo");
        a.add("bar");
        a.toString().shouldEqual("{\"bar\", \"foo\"}");
    }

    @("OrderedSet.add_single_element")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.toString().shouldEqual("{1}");
    }

    @("OrderedSet.to_string")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.toString().shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.remove_from_empty")
    @safe unittest
    {
        OrderedSet!int a;
        a.remove(1);
        a.toString().shouldEqual("{}");
    }

    @("OrderedSet.add_remove_elements")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.remove(2);
        a.toString().shouldEqual("{1, 3}");
    }

    @("OrderedSet.has_in_empty")
    @safe unittest
    {
        OrderedSet!int a;
        a.has(1).shouldBeFalse();
    }

    @("OrderedSet.in_operator_empty")
    @safe unittest
    {
        OrderedSet!int a;
        (1 in a).shouldBeFalse();
    }

    @("OrderedSet.add_large_and_negative")
    @safe unittest
    {
        OrderedSet!int a;
        a.add(-1000000);
        a.add(1000000);
        a.add(-500000);
        a.toString().shouldEqual("{-1000000, -500000, 1000000}");
    }

    @("OrderedSet.intersection_with_common")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        a.add(3);
        b.add(2);
        b.add(3);
        auto result = a.intersection(b);
        result.toString().shouldEqual("{2, 3}");
    }

    @("OrderedSet.union_with_duplicates")
    @safe unittest
    {
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(3);
        auto result = a + b;
        result.toString().shouldEqual("{1, 2, 3}");
    }
}
