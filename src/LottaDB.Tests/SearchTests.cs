using Lucene.Net.Index;
using Lucene.Net.Linq;
using Lucene.Net.Search;

namespace Lotta.Tests;

public class SearchTests
{
    // =====================================================================
    // Basic index behavior
    // =====================================================================

    [Fact]
    public async Task Search_EmptyIndex_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        Assert.Empty(db.Search<Actor>().ToList());
    }

    [Fact]
    public async Task Search_ReflectsSave()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);

        var results = db.Search<Actor>().ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task Search_ReflectsUpdate()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "Before" }, ct);
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "After" }, ct);

        var result = db.Search<Actor>().Single();
        Assert.Equal("After", result.DisplayName);
    }

    [Fact]
    public async Task Search_ReflectsDelete()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var actor = new Actor { Username = "gone", DisplayName = "Gone" };
        await db.SaveAsync(actor, ct);
        Assert.Single(db.Search<Actor>().ToList());

        await db.DeleteAsync(actor, ct);
        Assert.Empty(db.Search<Actor>().ToList());
    }

    [Fact]
    public async Task Search_ReflectsChangeAsync()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "u1", DisplayName = "Before", Counter = 1 }, ct);
        await db.ChangeAsync<Actor>("u1", a => { a.DisplayName = "After"; a.Counter = 99; return a; }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene indexes documents" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure tables store rows" }, ct);

        var results = db.Search<Note>("lucene").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchString_MultipleTerms_OrSemantics()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene search" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure storage" }, ct);
        await db.SaveAsync(new Note { NoteId = "n3", AuthorId = "carol", Content = "Redis caching" }, ct);

        var results = db.Search<Note>("lucene azure").ToList();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NoteId == "n1");
        Assert.Contains(results, r => r.NoteId == "n2");
    }

    [Fact]
    public async Task SearchString_Wildcard_MatchesPrefix()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure" }, ct);

        var results = db.Search<Note>("luc*").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchString_FieldQualified_TargetsNamedField()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "bob wrote something" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "alice wrote something" }, ct);

        // Qualified query restricts to the AuthorId field — content matches don't count.
        var results = db.Search<Note>("AuthorId:alice").ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchString_StemmedFormsMatch_WhenEnglishAnalyzer()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "running shoes are popular" }, ct);

        // EnglishAnalyzer stems "running" → "run" on both index and query sides of _content_.
        var results = db.Search<Note>("runs").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task SearchString_NonQueryableField_Unreachable()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello", NotQueryable = "xyzzy secret" }, ct);

        Assert.Empty(db.Search<Note>("xyzzy").ToList());
    }

    [Fact]
    public async Task SearchString_NoMatch_ReturnsEmpty()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello world" }, ct);

        Assert.Empty(db.Search<Note>("banana").ToList());
    }

    // =====================================================================
    // Search(predicate) / Search().Where(predicate) — LINQ predicates
    // =====================================================================

    [Fact]
    public async Task SearchPredicate_Equality_AnalyzedString()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

        var results = db.Search<Actor>(a => a.DisplayName == "alice").ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_Equality_KeyField()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "B" }, ct);

        var results = db.Search<Actor>(a => a.Username == "alice").ToList();
        Assert.Single(results);
    }

    [Fact]
    public async Task SearchPredicate_Contains_AnalyzedString()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "Lucene indexes text" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "Azure hosts tables" }, ct);

        var results = db.Search<Note>(n => n.Content.Contains("luce")).ToList();
        Assert.Single(results);
        Assert.Equal("n1", results[0].NoteId);
    }

    [Fact]
    public async Task SearchPredicate_Contains_NotAnalyzedString()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new NoteView { Id = "v1", NoteId = "n1", AuthorUsername = "alice-jones", AuthorDisplay = "x", Content = "x" }, ct);
        await db.SaveAsync(new NoteView { Id = "v2", NoteId = "n2", AuthorUsername = "bob-smith", AuthorDisplay = "y", Content = "y" }, ct);

        // AuthorUsername is NotAnalyzed — entire value is one token; substring wildcard still matches.
        var results = db.Search<NoteView>(v => v.AuthorUsername.Contains("alice")).ToList();
        Assert.Single(results);
        Assert.Equal("v1", results[0].Id);
    }

    [Fact]
    public async Task SearchPredicate_StartsWith()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Anderson" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob Baker" }, ct);

        var query = db.Search<Actor>(a => a.DisplayName.StartsWith("alice"));
        var results = query.ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_EndsWith()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice Admin" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob User" }, ct);

        var results = db.Search<Actor>(a => a.DisplayName.EndsWith("admin")).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_And()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", Counter = 30, DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", Counter = 30, DisplayName = "B" }, ct);
        await db.SaveAsync(new Actor { Username = "carol", Counter = 5, DisplayName = "C" }, ct);

        var results = db.Search<Actor>(a => a.Counter == 30 && a.Username == "alice").ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_Or()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", Counter = 1, DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", Counter = 50, DisplayName = "B" }, ct);
        await db.SaveAsync(new Actor { Username = "carol", Counter = 25, DisplayName = "C" }, ct);

        var results = db.Search<Actor>(a => a.Counter < 10 || a.Counter > 40)
            .OrderBy(a => a.Username)
            .ToList();
        Assert.Equal(["alice", "bob"], results.Select(a => a.Username).ToArray());
    }

    [Fact]
    public async Task SearchPredicate_NumericComparisons()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", Counter = 5, DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", Counter = 30, DisplayName = "B" }, ct);
        await db.SaveAsync(new Actor { Username = "carol", Counter = 30, DisplayName = "C" }, ct);

        Assert.Equal(2, db.Search<Actor>(a => a.Counter == 30).Count());
        Assert.Single(db.Search<Actor>(a => a.Counter < 10).ToList());
        Assert.Equal(2, db.Search<Actor>(a => a.Counter >= 10).Count());
    }

    [Fact]
    public async Task SearchPredicate_DateTimeRange()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var d1 = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var d2 = new DateTime(2024, 2, 1, 0, 0, 0, DateTimeKind.Utc);
        var d3 = new DateTime(2024, 3, 1, 0, 0, 0, DateTimeKind.Utc);
        await db.SaveAsync(new Actor { Username = "a", DisplayName = "a", CreatedAt = d1 }, ct);
        await db.SaveAsync(new Actor { Username = "b", DisplayName = "b", CreatedAt = d2 }, ct);
        await db.SaveAsync(new Actor { Username = "c", DisplayName = "c", CreatedAt = d3 }, ct);

        var results = db.Search<Actor>(a => a.CreatedAt >= d2 && a.CreatedAt < d3).ToList();
        Assert.Single(results);
        Assert.Equal("b", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_DateTimeOffsetEquality()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        var t = new DateTimeOffset(2024, 6, 1, 12, 0, 0, TimeSpan.Zero);
        await db.SaveAsync(new Actor { Username = "a", DisplayName = "a", LastSeenAt = t }, ct);
        await db.SaveAsync(new Actor { Username = "b", DisplayName = "b", LastSeenAt = t.AddHours(1) }, ct);

        var results = db.Search<Actor>(a => a.LastSeenAt == t).ToList();
        Assert.Single(results);
        Assert.Equal("a", results[0].Username);
    }

    [Fact]
    public async Task SearchPredicate_EquivalentToWhereLambda()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "Alice" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "Bob" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "lorem ipsum" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "alice wrote this" }, ct);
        await db.SaveAsync(new Note { NoteId = "n3", AuthorId = "carol", Content = "unrelated text" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "hello" }, ct);

        Assert.Empty(db.Search<Note>().Where(n => n.AnyField() == "nonexistent").ToList());
    }

    // =====================================================================
    // Search().Where(Query) — raw Lucene Query clause
    // =====================================================================

    [Fact]
    public async Task SearchWhereQuery_AppliesRawTermQuery()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "B" }, ct);

        Query q = new TermQuery(new Term("Username", "alice"));
        var results = db.Search<Actor>().Where(q).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchWhereQuery_AppliesCanonicalKeyQuery()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", DisplayName = "B" }, ct);

        Query q = new TermQuery(new Term("_key_", "alice"));
        var results = db.Search<Actor>().Where(q).ToList();
        Assert.Single(results);
        Assert.Equal("alice", results[0].Username);
    }

    [Fact]
    public async Task SearchWhereQuery_ComposesWithLinq()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "alice", Counter = 10, DisplayName = "A" }, ct);
        await db.SaveAsync(new Actor { Username = "alice2", Counter = 99, DisplayName = "A2" }, ct);
        await db.SaveAsync(new Actor { Username = "bob", Counter = 10, DisplayName = "B" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Note { NoteId = "n1", AuthorId = "alice", Content = "lucene indexes" }, ct);
        await db.SaveAsync(new Note { NoteId = "n2", AuthorId = "bob", Content = "lucene searches" }, ct);
        await db.SaveAsync(new Note { NoteId = "n3", AuthorId = "alice", Content = "azure tables" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "c", Counter = 30, DisplayName = "c" }, ct);
        await db.SaveAsync(new Actor { Username = "a", Counter = 10, DisplayName = "a" }, ct);
        await db.SaveAsync(new Actor { Username = "b", Counter = 20, DisplayName = "b" }, ct);

        var order = db.Search<Actor>().OrderBy(a => a.Counter).Select(a => a.Username).ToList();
        Assert.Equal(["a", "b", "c"], order);
    }

    [Fact]
    public async Task Search_OrderByNumeric_Descending()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        await db.SaveAsync(new Actor { Username = "c", Counter = 30, DisplayName = "c" }, ct);
        await db.SaveAsync(new Actor { Username = "a", Counter = 10, DisplayName = "a" }, ct);
        await db.SaveAsync(new Actor { Username = "b", Counter = 20, DisplayName = "b" }, ct);

        var order = db.Search<Actor>().OrderByDescending(a => a.Counter).Select(a => a.Username).ToList();
        Assert.Equal(["c", "b", "a"], order);
    }

    [Fact]
    public async Task Search_Skip_And_Take_Paginates()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Username = $"u{i:D2}", Counter = i, DisplayName = $"u{i:D2}" }, ct);

        var page = db.Search<Actor>().OrderBy(a => a.Counter).Skip(3).Take(3)
            .Select(a => a.Username).ToList();
        Assert.Equal(["u03", "u04", "u05"], page);
    }

    [Fact]
    public async Task Search_Take_Limits()
    {
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(cancellationToken: ct);
        for (int i = 0; i < 10; i++)
            await db.SaveAsync(new Actor { Username = $"u{i:D2}", DisplayName = $"u{i:D2}" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts => opts.Store<TodoLike>(), cancellationToken: ct);
        await db.SaveAsync(new TodoLike { Title = "Buy groceries", Notes = "milk and bread" }, ct);
        await db.SaveAsync(new TodoLike { Title = "Write report", Notes = "quarterly numbers" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts => opts.Store<TodoLike>(), cancellationToken: ct);
        await db.SaveAsync(new TodoLike { Title = "Buy groceries", Notes = "milk and bread" }, ct);
        await db.SaveAsync(new TodoLike { Title = "Write report", Notes = "quarterly numbers" }, ct);

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
        var ct = TestContext.Current.CancellationToken;
        using var db = await LottaDBFixture.CreateDbAsync(opts => opts.Store<TodoLike>(), cancellationToken: ct);
        await db.SaveAsync(new TodoLike { Title = "Buy groceries", Notes = "milk", IsDone = false }, ct);
        await db.SaveAsync(new TodoLike { Title = "Buy flowers", Notes = "for mom", IsDone = true }, ct);

        var results = db.Search<TodoLike>("buy")
            .Where(t => t.IsDone == false)
            .OrderBy(t => t.IsDone)
            .ThenByDescending(t => t.Created)
            .ToList();

        Assert.Single(results);
        Assert.Equal("Buy groceries", results[0].Title);
    }
}
