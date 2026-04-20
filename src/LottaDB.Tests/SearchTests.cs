using Lucene.Net.Index;
using Lucene.Net.Linq;
using Lucene.Net.Search;

#pragma warning disable xUnit1051 // tests are synchronous against in-memory storage; CT adds noise

namespace Lotta.Tests;

public class SearchTests
{
    // =====================================================================
    // Basic index behavior
    // =====================================================================

    [Fact]
    public async Task Search_EmptyIndex_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        Assert.Empty(db.Search<Actor>().ToList());
    }

    [Fact]
    public async Task Search_ReflectsSave()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Search_ReflectsUpdate()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "Before" });
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "After" });

        var result = db.Search<Actor>().Single();
        Assert.Equal("After", result.DisplayName);
    }

    [Fact]
    public async Task Search_ReflectsDelete()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var actor = new Actor { Username = "gone", DisplayName = "Gone" };
        await db.SaveAsync(actor);
        Assert.Single(db.Search<Actor>().ToList());

        await db.DeleteAsync(actor);
        Assert.Empty(db.Search<Actor>().ToList());
    }

    [Fact]
    public async Task Search_ReflectsChangeAsync()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "Before", Counter = 1 });
        await db.ChangeAsync<Actor>("u1", a => { a.DisplayName = "After"; a.Counter = 99; return a; });

        var result = db.Search<Actor>().Single();
        Assert.Equal("After", result.DisplayName);
        Assert.Equal(99, result.Counter);
    }

    // =====================================================================
    // Search(string) — free-text query against _content_ field
    // =====================================================================

    [Fact]
    public async Task SearchString_SingleTerm_HitsAnalyzedContent()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene indexes documents" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure tables store rows" });

        var results = db.Search<Note>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchString_MultipleTerms_OrSemantics()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene search" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure storage" });
        await db.SaveAsync(new Note { NoteId = "n3", AuthorId = "carol", Content = "Redis caching" });

        var results = db.Search<Note>("lucene azure").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NoteId == "n1");
        Assert.Contains(results, r => r.NoteId == "n2");
    }

    [Fact]
    public async Task SearchString_Wildcard_MatchesPrefix()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure" });

        var results = db.Search<Note>("luc*").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchString_FieldQualified_TargetsNamedField()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "bob wrote something" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "alice wrote something" });

        // Qualified query restricts to the AuthorId field — content matches don't count.
        var results = db.Search<Note>("AuthorId:alice").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchString_StemmedFormsMatch_WhenEnglishAnalyzer()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "running shoes are popular" });

        // EnglishAnalyzer stems "running" → "run" on both index and query sides of _content_.
        var results = db.Search<Note>("runs").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task SearchString_NonQueryableField_Unreachable()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello", NotQueryable = "xyzzy secret" });

        Assert.Empty(db.Search<Note>("xyzzy").ToList());
    }

    [Fact]
    public async Task SearchString_NoMatch_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello world" });

        Assert.Empty(db.Search<Note>("banana").ToList());
    }

    // =====================================================================
    // Search(predicate) / Search().Where(predicate) — LINQ predicates
    // =====================================================================

    [Fact]
    public async Task SearchPredicate_Equality_AnalyzedString()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var results = db.Search<Actor>(a => a.DisplayName == "alice").ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_Equality_KeyField()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "B" });

        var results = db.Search<Actor>(a => a.Username == "alice").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task SearchPredicate_Contains_AnalyzedString()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene indexes text" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure hosts tables" });

        var results = db.Search<Note>(n => n.Content.Contains("luce")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchPredicate_Contains_NotAnalyzedString()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new NoteView { Id = "v1", NoteId = "n1", AuthorUsername = "alice-jones", AuthorDisplay = "x", Content = "x" });
        await db.SaveAsync(new NoteView { Id = "v2", NoteId = "n2", AuthorUsername = "bob-smith", AuthorDisplay = "y", Content = "y" });

        // AuthorUsername is NotAnalyzed — entire value is one token; substring wildcard still matches.
        var results = db.Search<NoteView>(v => v.AuthorUsername.Contains("alice")).ToList();
        Assert.Single(results);
        Assert.Equal("v1", results[0].Id);
    }

    [Fact]
    public async Task SearchPredicate_StartsWith()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Anderson" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob Baker" });

        var query = db.Search<Actor>(a => a.DisplayName.StartsWith("alice"));
        var results = query.ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_EndsWith()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Admin" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob User" });

        var results = db.Search<Actor>(a => a.DisplayName.EndsWith("admin")).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_And()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", Counter = 30, DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "bob", Counter = 30, DisplayName = "B" });
        await db.SaveAsync(new Actor { Username = "carol", Counter = 5, DisplayName = "C" });

        var results = db.Search<Actor>(a => a.Counter == 30 && a.Username == "alice").ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_Or()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", Counter = 1, DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "bob", Counter = 50, DisplayName = "B" });
        await db.SaveAsync(new Actor { Username = "carol", Counter = 25, DisplayName = "C" });

        var results = db.Search<Actor>(a => a.Counter < 10 || a.Counter > 40)
            .OrderBy(a => a.Username)
            .ToList();
        Assert.Equal(["alice", "bob"], results.Select(a => a.Username).ToArray());
    }

    [Fact]
    public async Task SearchPredicate_NumericComparisons()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", Counter = 5, DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "bob", Counter = 30, DisplayName = "B" });
        await db.SaveAsync(new Actor { Username = "carol", Counter = 30, DisplayName = "C" });

        Assert.Equal(2, db.Search<Actor>(a => a.Counter == 30).Count());
        Assert.Single(db.Search<Actor>(a => a.Counter < 10).ToList());
        Assert.Equal(2, db.Search<Actor>(a => a.Counter >= 10).Count());
    }

    [Fact]
    public async Task SearchPredicate_DateTimeRange()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var d1 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var d2 = new DateTime(2024, 2, 1, 0, 0, 0, DateTimeKind.Utc);
        var d3 = new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc);
        await db.SaveAsync(new Actor { Username = "a", DisplayName = "a", CreatedAt = d1 });
        await db.SaveAsync(new Actor { Username = "b", DisplayName = "b", CreatedAt = d2 });
        await db.SaveAsync(new Actor { Username = "c", DisplayName = "c", CreatedAt = d3 });

        var results = db.Search<Actor>(a => a.CreatedAt >= d2 && a.CreatedAt < d3).ToList();
        Assert.Single(results);
        Assert.Equal("b", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_DateTimeOffsetEquality()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        var t = new DateTimeOffset(2024, 6, 1, 12, 0, 0, TimeSpan.Zero);
        await db.SaveAsync(new Actor { Username = "a", DisplayName = "a", LastSeenAt = t });
        await db.SaveAsync(new Actor { Username = "b", DisplayName = "b", LastSeenAt = t.AddHours(1) });

        var results = db.Search<Actor>(a => a.LastSeenAt == t).ToList();
        Assert.Single(results);
        Assert.Equal("a", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_EquivalentToWhereLambda()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" });

        var a = db.Search<Actor>(x => x.Username == "alice").ToList();
        var b = db.Search<Actor>().Where(x => x.Username == "alice").ToList();
        Assert.Equal(a.Count, b.Count);
        Assert.Equal(a[0].Username, b[0].Username);
    }

    // =====================================================================
    // AnyField — multi-field search via LuceneMethods.AnyField
    // =====================================================================

    [Fact]
    public async Task SearchAnyField_MatchesAcrossFields()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "lorem ipsum" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "alice wrote this" });
        await db.SaveAsync(new Note { NoteId = "n3", AuthorId = "carol", Content = "unrelated text" });

        // "alice" hits the AuthorId field in n1 and the Content field in n2.
        var x = db.Search<Note>().ToList();
        var results = db.Search<Note>().Where(n => n.AnyField() == "alice").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NoteId == "n1");
        Assert.Contains(results, r => r.NoteId == "n2");
    }

    [Fact]
    public async Task SearchAnyField_NoMatch_ReturnsEmpty()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" });

        Assert.Empty(db.Search<Note>().Where(n => n.AnyField() == "nonexistent").ToList());
    }

    // =====================================================================
    // Search().Where(Query) — raw Lucene Query clause
    // =====================================================================

    [Fact]
    public async Task SearchWhereQuery_AppliesRawTermQuery()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "B" });

        Query q = new TermQuery(new Term("Username", "alice"));
        var results = db.Search<Actor>().Where(q).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchWhereQuery_AppliesCanonicalKeyQuery()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "B" });

        Query q = new TermQuery(new Term("_key_", "alice"));
        var results = db.Search<Actor>().Where(q).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchWhereQuery_ComposesWithLinq()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "alice", Counter = 10, DisplayName = "A" });
        await db.SaveAsync(new Actor { Username = "alice2", Counter = 99, DisplayName = "A2" });
        await db.SaveAsync(new Actor { Username = "bob", Counter = 10, DisplayName = "B" });

        Query q = new WildcardQuery(new Term("Username", "alice*"));
        var results = db.Search<Actor>().Where(q).Where(a => a.Counter < 50).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    // =====================================================================
    // Hybrid: Search(queryString).Where(predicate)
    // =====================================================================

    [Fact]
    public async Task SearchHybrid_StringThenLinq_NarrowsResults()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "lucene indexes" });
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "lucene searches" });
        await db.SaveAsync(new Note { NoteId = "n3", AuthorId = "alice", Content = "azure tables" });

        // Free-text "lucene" hits n1 and n2; LINQ filter narrows to AuthorId == "alice".
        var results = db.Search<Note>("lucene").Where(n => n.AuthorId == "alice").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    // =====================================================================
    // Ordering and paging
    // =====================================================================

    [Fact]
    public async Task Search_OrderByNumeric_Ascending()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "c", Counter = 30, DisplayName = "c" });
        await db.SaveAsync(new Actor { Username = "a", Counter = 10, DisplayName = "a" });
        await db.SaveAsync(new Actor { Username = "b", Counter = 20, DisplayName = "b" });

        var order = db.Search<Actor>().OrderBy(a => a.Counter).Select(a => a.Username).ToList();
        Assert.Equal(["a", "b", "c"], order);
    }

    [Fact]
    public async Task Search_OrderByNumeric_Descending()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        await db.SaveAsync(new Actor { Username = "c", Counter = 30, DisplayName = "c" });
        await db.SaveAsync(new Actor { Username = "a", Counter = 10, DisplayName = "a" });
        await db.SaveAsync(new Actor { Username = "b", Counter = 20, DisplayName = "b" });

        var order = db.Search<Actor>().OrderByDescending(a => a.Counter).Select(a => a.Username).ToList();
        Assert.Equal(["c", "b", "a"], order);
    }

    [Fact]
    public async Task Search_Skip_And_Take_Paginates()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Username = $"u{i:D2}", Counter = i, DisplayName = $"u{i:D2}" });

        var page = db.Search<Actor>().OrderBy(a => a.Counter).Skip(3).Take(3)
            .Select(a => a.Username).ToList();
        Assert.Equal(["u03", "u04", "u05"], page);
    }

    [Fact]
    public async Task Search_Take_Limits()
    {
        using var db = await LottaDBFixture.CreateDbAsync();
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Username = $"u{i:D2}", DisplayName = $"u{i:D2}" });

        Assert.Equal(3, db.Search<Actor>().Take(3).ToList().Count);
    }

    // =====================================================================
    // Regression: mirrors the TodoApp sample — free-text search over Title/Notes
    // combined with an OrderBy on a NotAnalyzed bool field.
    // =====================================================================

    public class TodoLike
    {
        [Key(Mode = KeyMode.Auto)]
        public string Id { get; set; } = "";

        [Queryable]
        public string Title { get; set; } = "";

        [Queryable]
        public string Notes { get; set; } = "";

        [Queryable]
        public bool IsDone { get; set; }

        [Queryable]
        public DateTimeOffset Created { get; set; } = DateTimeOffset.UtcNow;
    }

    [Fact]
    public async Task TodoApp_FreeTextSearch_MatchesTitleToken()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts => opts.Store<TodoLike>());
        await db.SaveAsync(new TodoLike { Title = "Buy groceries", Notes = "milk and bread" });
        await db.SaveAsync(new TodoLike { Title = "Write report", Notes = "quarterly numbers" });

        var results = db.Search<TodoLike>("groceries")
            .OrderBy(t => t.IsDone)
            .ThenByDescending(t => t.Created)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Buy groceries", results[0].Title);
    }

    [Fact]
    public async Task TodoApp_FreeTextSearch_MatchesNotesToken()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts => opts.Store<TodoLike>());
        await db.SaveAsync(new TodoLike { Title = "Buy groceries", Notes = "milk and bread" });
        await db.SaveAsync(new TodoLike { Title = "Write report", Notes = "quarterly numbers" });

        var results = db.Search<TodoLike>("milk")
            .OrderBy(t => t.IsDone)
            .ThenByDescending(t => t.Created)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Buy groceries", results[0].Title);
    }


    [Fact]
    public async Task TodoApp_FreeTextSearch_WithBoolFilter()
    {
        using var db = await LottaDBFixture.CreateDbAsync(opts => opts.Store<TodoLike>());
        await db.SaveAsync(new TodoLike { Title = "Buy groceries", Notes = "milk", IsDone = false });
        await db.SaveAsync(new TodoLike { Title = "Buy flowers", Notes = "for mom", IsDone = true });

        var results = db.Search<TodoLike>("buy")
            .Where(t => t.IsDone == false)
            .OrderBy(t => t.IsDone)
            .ThenByDescending(t => t.Created)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Buy groceries", results[0].Title);
    }
}
