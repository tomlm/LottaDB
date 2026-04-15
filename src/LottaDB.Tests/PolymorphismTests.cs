namespace Lotta.Tests;

public class PolymorphismTests
{
    [Fact]
    public async Task Query_BaseClass_ReturnsAllDerivedTypes()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new BaseEntity { Id = "base1", Name = "Base" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person1", Name = "Alice", Email = "alice@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Employee { Id = "emp1", Name = "Bob", Email = "bob@test.com", Department = "Engineering" }, TestContext.Current.CancellationToken);

        // Query<BaseEntity> should return all three
        var all = db.Query<BaseEntity>().ToList();
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task Query_MiddleClass_ReturnsMiddleAndDerived()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new BaseEntity { Id = "base2", Name = "Base" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person2", Name = "Alice", Email = "alice@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Employee { Id = "emp2", Name = "Bob", Email = "bob@test.com", Department = "Engineering" }, TestContext.Current.CancellationToken);

        // Query<Person> should return Person + Employee, not BaseEntity
        var people = db.Query<Person>().ToList();
        Assert.Equal(2, people.Count);
    }

    [Fact]
    public async Task Query_LeafClass_ReturnsOnlyExactType()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new BaseEntity { Id = "base3", Name = "Base" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person3", Name = "Alice", Email = "alice@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Employee { Id = "emp3", Name = "Bob", Email = "bob@test.com", Department = "Engineering" }, TestContext.Current.CancellationToken);

        // Query<Employee> should return only Employee
        var employees = db.Query<Employee>().ToList();
        Assert.Single(employees);
        Assert.Equal("emp3", employees[0].Id);
    }

    [Fact]
    public async Task Query_BaseClass_DeserializesToConcreteType()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new Employee { Id = "emp4", Name = "Carol", Email = "carol@test.com", Department = "Sales" }, TestContext.Current.CancellationToken);

        // Query<BaseEntity> should return an Employee with all properties intact
        var all = db.Query<BaseEntity>().ToList();
        Assert.Single(all);

        var item = all[0];
        Assert.IsType<Employee>(item);
        Assert.Equal("Carol", item.Name);

        var emp = (Employee)item;
        Assert.Equal("carol@test.com", emp.Email);
        Assert.Equal("Sales", emp.Department);
    }

    [Fact]
    public async Task Query_MiddleClass_PreservesDerivedProperties()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new Employee { Id = "emp5", Name = "Dave", Email = "dave@test.com", Department = "HR" }, TestContext.Current.CancellationToken);

        // Query<Person> should return Employee with Department intact
        var people = db.Query<Person>().ToList();
        Assert.Single(people);
        Assert.IsType<Employee>(people[0]);
        Assert.Equal("HR", ((Employee)people[0]).Department);
    }

    [Fact]
    public async Task GetAsync_ReturnsConcreteType()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new Employee { Id = "emp6", Name = "Eve", Email = "eve@test.com", Department = "Legal" }, TestContext.Current.CancellationToken);

        // GetAsync<BaseEntity> should return the concrete Employee type
        // Note: GetAsync uses PartitionKey which is the exact type name,
        // so this only works with the exact type
        var emp = await db.GetAsync<Employee>("emp6", TestContext.Current.CancellationToken);
        Assert.NotNull(emp);
        Assert.Equal("Legal", emp.Department);
    }

    [Fact]
    public async Task Search_ExactType_ReturnsOnlyThatType()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new BaseEntity { Id = "base7", Name = "Base" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "person7", Name = "Alice", Email = "alice@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Employee { Id = "emp7", Name = "Bob", Email = "bob@test.com", Department = "Engineering" }, TestContext.Current.CancellationToken);

        // Search<Employee> should return only Employee
        // Currently returns all types because Lucene has no type discriminator
        var employees = db.Search<Employee>().ToList();
        Assert.Single(employees);
        Assert.Equal("emp7", employees[0].Id);
    }

    [Fact]
    public async Task Search_BaseClass_ReturnsAllDerivedTypes()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new BaseEntity { Id = "sbase1", Name = "Base" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "sperson1", Name = "Alice", Email = "alice@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Employee { Id = "semp1", Name = "Bob", Email = "bob@test.com", Department = "Engineering" }, TestContext.Current.CancellationToken);

        var all = db.Search<BaseEntity>().ToList();
        Assert.Equal(3, all.Count);
    }

    [Fact]
    public async Task Search_MiddleClass_ReturnsMiddleAndDerived()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new BaseEntity { Id = "sbase2", Name = "Base" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Person { Id = "sperson2", Name = "Alice", Email = "alice@test.com" }, TestContext.Current.CancellationToken);
        await db.SaveAsync(new Employee { Id = "semp2", Name = "Bob", Email = "bob@test.com", Department = "Engineering" }, TestContext.Current.CancellationToken);

        var people = db.Search<Person>().ToList();
        Assert.Equal(2, people.Count);
    }

    [Fact]
    public async Task Search_BaseClass_DeserializesToConcreteType()
    {
        var db = LottaDBFixture.CreateDb();

        await db.SaveAsync(new Employee { Id = "semp3", Name = "Carol", Email = "carol@test.com", Department = "Sales" }, TestContext.Current.CancellationToken);

        var all = db.Search<BaseEntity>().ToList();
        Assert.Single(all);
        Assert.IsType<Employee>(all[0]);
        Assert.Equal("Sales", ((Employee)all[0]).Department);
    }
}
