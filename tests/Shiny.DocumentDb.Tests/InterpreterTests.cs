using System.Linq.Expressions;
using Shiny.DocumentDb.Internal.Query;
using Shiny.DocumentDb.Tests.Fixtures;
using Xunit;

namespace Shiny.DocumentDb.Tests;

/// <summary>
/// Direct unit tests for the compile-free <see cref="ExpressionInterpreter"/> — covers the node branches
/// (arithmetic, bitwise, coalesce, conditional, member chains, method calls, NewExpression, captured
/// values) that the provider suites only reach indirectly.
/// </summary>
public class InterpreterTests
{
    static Func<T, TR> I<T, TR>(Expression<Func<T, TR>> e) => ExpressionInterpreter.Interpret(e);

    [Fact]
    public void Comparisons_And_Logical()
    {
        var u = new User { Name = "Alice", Age = 25 };
        Assert.True(I<User, bool>(x => x.Age == 25)(u));
        Assert.True(I<User, bool>(x => x.Age > 18 && x.Name == "Alice")(u));
        Assert.True(I<User, bool>(x => x.Age < 10 || x.Name == "Alice")(u));
        Assert.True(I<User, bool>(x => !(x.Age == 30))(u));
        Assert.False(I<User, bool>(x => x.Age >= 26)(u));
    }

    [Fact]
    public void Arithmetic()
    {
        var u = new User { Age = 20 };
        Assert.True(I<User, bool>(x => x.Age + 5 == 25)(u));
        Assert.True(I<User, bool>(x => x.Age - 5 == 15)(u));
        Assert.True(I<User, bool>(x => x.Age * 2 == 40)(u));
        Assert.True(I<User, bool>(x => x.Age % 3 == 2)(u));
    }

    [Fact]
    public void Bitwise_Flags()
    {
        var a = new Account { Permissions = Permissions.Read | Permissions.Write };
        Assert.True(I<Account, bool>(x => (x.Permissions & Permissions.Write) == Permissions.Write)(a));
        Assert.True(I<Account, bool>(x => x.Permissions.HasFlag(Permissions.Read))(a));
        Assert.False(I<Account, bool>(x => x.Permissions.HasFlag(Permissions.Delete))(a));
    }

    [Fact]
    public void Coalesce_And_Conditional()
    {
        Assert.Equal("none", I<User, string>(x => x.Email ?? "none")(new User { Email = null }));
        Assert.Equal("a@b", I<User, string>(x => x.Email ?? "none")(new User { Email = "a@b" }));
        Assert.Equal("adult", I<User, string>(x => x.Age >= 18 ? "adult" : "minor")(new User { Age = 30 }));
        Assert.Equal("minor", I<User, string>(x => x.Age >= 18 ? "adult" : "minor")(new User { Age = 5 }));
    }

    [Fact]
    public void NestedMember_And_StringMethods()
    {
        var o = new Order { ShippingAddress = new Address { City = "Portland" } };
        Assert.True(I<Order, bool>(x => x.ShippingAddress.City == "Portland")(o));
        Assert.True(I<Order, bool>(x => x.ShippingAddress.City.ToUpper() == "PORTLAND")(o));
        Assert.True(I<Order, bool>(x => x.ShippingAddress.City.Length == 8)(o));
    }

    [Fact]
    public void NewExpression_Projection()
    {
        var u = new User { Name = "Alice", Email = "a@b" };
        var proj = I<User, UserSummary>(x => new UserSummary { Name = x.Name, Email = x.Email })(u);
        Assert.Equal("Alice", proj.Name);
        Assert.Equal("a@b", proj.Email);
    }

    [Fact]
    public void CapturedVariable_And_Collections()
    {
        var target = "Alice";
        Assert.True(I<User, bool>(x => x.Name == target)(new User { Name = "Alice" }));

        var o = new Order { Tags = ["priority", "retail"] };
        Assert.True(I<Order, bool>(x => x.Tags.Any(t => t == "priority"))(o));
        Assert.True(I<Order, bool>(x => x.Tags.Count() == 2)(o));
    }
}
