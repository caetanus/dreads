module tests.ordered_set_tests;

version (unittest)
{
    import std.stdio;
    import dreads.containers;

    @("OrderedSet.add_elements")
    @safe unittest
    {
        // Testa a adição de elementos
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.toString().shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.add_duplicates")
    @safe unittest
    {
        // Testa a adição de elementos duplicados
        OrderedSet!int a;
        a.add(1);
        a.add(1);
        a.add(2);
        a.add(3);
        a.toString().shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.length")
    @safe unittest
    {
        // Testa o tamanho do conjunto
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.length().shouldEqual(3);
    }

    @("OrderedSet.remove_elements")
    @safe unittest
    {
        // Testa a remoção de elementos
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.remove(1);
        a.toString().shouldEqual("{2}");
    }

    @("OrderedSet.has_element")
    @safe unittest
    {
        // Testa se o conjunto contém um item
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.has(1).shouldBeTrue();
        a.has(3).shouldBeFalse();
    }

    @("OrderedSet.in_operator")
    @safe unittest
    {
        // Testa a operação de inclusão "in"
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        // Descomentado se OrderedSet implementar opBinaryRight!"in"
        //(1 in a).shouldBeTrue();
        //(3 in a).shouldBeFalse();
    }

    @("OrderedSet.equality")
    @safe unittest
    {
        // Testa a verificação de igualdade
        OrderedSet!int a;
        OrderedSet!int b;
        a.add(1);
        a.add(2);
        b.add(2);
        b.add(1);
        // Descomentado se OrderedSet implementar opEquals
        //(a == b).shouldBeTrue();
    }

    @("OrderedSet.union_operation")
    @safe unittest
    {
        // Testa a operação de união
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
        // Testa a operação de diferença
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
        // Testa a operação de interseção
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
        // Testa a operação de diferença simétrica
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
        // Testa se um conjunto é subconjunto de outro
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
        // Testa se um conjunto é superconjunto de outro
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
        // Testa a operação de união com conjuntos vazios
        OrderedSet!int a;
        OrderedSet!int b;
        auto result = a + b;
        result.toString().shouldEqual("{}");
    }

    @("OrderedSet.intersection_with_empty")
    @safe unittest
    {
        // Testa a interseção com conjuntos vazios
        OrderedSet!int a;
        OrderedSet!int b;
        auto result = a.intersection(b);
        result.toString().shouldEqual("{}");
    }

    @("OrderedSet.difference_with_empty")
    @safe unittest
    {
        // Testa a diferença com conjuntos vazios
        OrderedSet!int a;
        OrderedSet!int b;
        auto result = a - b;
        result.toString().shouldEqual("{}");
    }

    @("OrderedSet.add_negative_elements")
    @safe unittest
    {
        // Testa a adição de elementos negativos
        OrderedSet!int a;
        a.add(-1);
        a.add(-2);
        a.toString().shouldEqual("{-2, -1}");
    }

    @("OrderedSet.add_large_elements")
    @safe unittest
    {
        // Testa a adição de elementos grandes
        OrderedSet!int a;
        a.add(1000000);
        a.add(5000000);
        a.toString().shouldEqual("{1000000, 5000000}");
    }

    @("OrderedSet.empty_subset_check")
    @safe unittest
    {
        // Testa a verificação de subset com conjuntos vazios
        OrderedSet!int a;
        OrderedSet!int b;
        a.isSubsetOf(b).shouldBeTrue();
    }

    @("OrderedSet.empty_superset_check")
    @safe unittest
    {
        // Testa a verificação de superset com conjuntos vazios
        OrderedSet!int a;
        OrderedSet!int b;
        b.isSuperSetOf(a).shouldBeTrue();
    }

    @("OrderedSet.empty_equality")
    @safe unittest
    {
        // Testa a comparação de igualdade com conjuntos vazios
        OrderedSet!int a;
        OrderedSet!int b;
        // Descomentado se OrderedSet implementar opEquals
        //(a == b).shouldBeTrue();
    }

    @("OrderedSet.add_remove_duplicates")
    @safe unittest
    {
        // Testa a operação de inserção e remoção com elementos repetidos
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
        // Testa a operação de inserção com diferentes tipos
        OrderedSet!string a;
        a.add("foo");
        a.add("bar");
        a.toString().shouldEqual("{bar, foo}");
    }

    @("OrderedSet.add_single_element")
    @safe unittest
    {
        // Testa a operação de add com um único item
        OrderedSet!int a;
        a.add(1);
        a.toString().shouldEqual("{1}");
    }

    @("OrderedSet.to_string")
    @safe unittest
    {
        // Testa a conversão para string
        OrderedSet!int a;
        a.add(1);
        a.add(2);
        a.add(3);
        a.toString().shouldEqual("{1, 2, 3}");
    }

    @("OrderedSet.remove_from_empty")
    @safe unittest
    {
        // Testa a remoção de elementos do conjunto vazio
        OrderedSet!int a;
        a.remove(1);
        a.toString().shouldEqual("{}");
    }

    @("OrderedSet.add_remove_elements")
    @safe unittest
    {
        // Testa a adição e remoção de elementos no conjunto
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
        // Testa a operação "has" no conjunto vazio
        OrderedSet!int a;
        a.has(1).shouldBeFalse();
    }

    @("OrderedSet.in_operator_empty")
    @safe unittest
    {
        // Testa a operação "in" no conjunto vazio
        OrderedSet!int a;
        // Descomentado se OrderedSet implementar opBinaryRight!"in"
        //(1 in a).shouldBeFalse();
    }

    @("OrderedSet.add_large_and_negative")
    @safe unittest
    {
        // Testa a adição de elementos grandes e negativos
        OrderedSet!int a;
        a.add(-1000000);
        a.add(1000000);
        a.add(-500000);
        a.toString().shouldEqual("{-1000000, -500000, 1000000}");
    }

    @("OrderedSet.intersection_with_common")
    @safe unittest
    {
        // Testa a operação de interseção com elementos comuns
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
        // Testa a união com elementos repetidos
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
