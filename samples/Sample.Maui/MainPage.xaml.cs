using Shiny.DocumentDb;

namespace Sample.Maui;

public partial class MainPage : ContentPage
{
    readonly IDocumentStore store;
    string selectedType = "Customer";

    public MainPage(IDocumentStore store)
    {
        InitializeComponent();
        this.store = store;
        _ = InitializeAsync();
    }

    async Task InitializeAsync()
    {
        await SeedData.SeedAsync(store);
        await LoadCustomersAsync();
    }

    void OnCustomersClicked(object? sender, EventArgs e)
    {
        selectedType = "Customer";
        UpdateTabStyles();
        _ = LoadCustomersAsync();
    }

    void OnOrdersClicked(object? sender, EventArgs e)
    {
        selectedType = "Order";
        UpdateTabStyles();
        _ = LoadOrdersAsync();
    }

    void UpdateTabStyles()
    {
        var primary = (Color)Application.Current!.Resources["Primary"];
        var inactiveBackground = Application.Current.RequestedTheme == AppTheme.Dark
            ? Color.FromArgb("#2A2A4A")
            : Color.FromArgb("#E8E8E8");
        var inactiveText = Application.Current.RequestedTheme == AppTheme.Dark
            ? Color.FromArgb("#AAAAAA")
            : Color.FromArgb("#555555");

        if (selectedType == "Customer")
        {
            BtnCustomers.BackgroundColor = primary;
            BtnCustomers.TextColor = Colors.White;
            BtnOrders.BackgroundColor = inactiveBackground;
            BtnOrders.TextColor = inactiveText;
        }
        else
        {
            BtnOrders.BackgroundColor = primary;
            BtnOrders.TextColor = Colors.White;
            BtnCustomers.BackgroundColor = inactiveBackground;
            BtnCustomers.TextColor = inactiveText;
        }
    }

    async void OnQueryExecuted(object? sender, EventArgs e)
    {
        var where = SqlEntry.Text?.Trim();
        if (string.IsNullOrEmpty(where))
        {
            if (selectedType == "Customer")
                await LoadCustomersAsync();
            else
                await LoadOrdersAsync();
            return;
        }

        try
        {
            StatusLabel.Text = "";
            if (selectedType == "Customer")
            {
                var results = await store.Query<Customer>(where);
                DisplayCustomers(results);
            }
            else
            {
                var results = await store.Query<Order>(where);
                DisplayOrders(results);
            }
        }
        catch (Exception ex)
        {
            StatusLabel.Text = ex.Message;
        }
    }

    async Task LoadCustomersAsync()
    {
        var results = await store.Query<Customer>().ToList();
        DisplayCustomers(results);
    }

    async Task LoadOrdersAsync()
    {
        var results = await store.Query<Order>().ToList();
        DisplayOrders(results);
    }

    void DisplayCustomers(IReadOnlyList<Customer> customers)
    {
        CountLabel.Text = $"{customers.Count} results";
        SetHeader("Id", "Name", "Age", "Email", "City");
        ResultsView.ItemTemplate = new DataTemplate(() =>
        {
            var grid = new Grid
            {
                ColumnDefinitions = CreateColumns(5),
                Padding = new Thickness(12, 8)
            };
            grid.Add(CreateLabel("Id", true), 0);
            grid.Add(CreateLabel("Name"), 1);
            grid.Add(CreateLabel("Age"), 2);
            grid.Add(CreateLabel("Email"), 3);
            grid.Add(CreateLabel("City"), 4);
            return grid;
        });
        ResultsView.ItemsSource = customers;
    }

    void DisplayOrders(IReadOnlyList<Order> orders)
    {
        CountLabel.Text = $"{orders.Count} results";
        SetHeader("Id", "Customer", "Status", "Total", "Created", "Lines");
        ResultsView.ItemTemplate = new DataTemplate(() =>
        {
            var grid = new Grid
            {
                ColumnDefinitions = CreateColumns(6),
                Padding = new Thickness(12, 8)
            };
            grid.Add(CreateLabel("Id", true), 0);
            grid.Add(CreateLabel("CustomerName"), 1);
            grid.Add(CreateLabel("Status"), 2);
            grid.Add(CreateLabel("Total"), 3);
            grid.Add(CreateLabel("CreatedAt"), 4);
            grid.Add(CreateLabel("Lines.Count"), 5);
            return grid;
        });
        ResultsView.ItemsSource = orders;
    }

    void SetHeader(params string[] columns)
    {
        HeaderGrid.ColumnDefinitions = CreateColumns(columns.Length);
        HeaderGrid.Children.Clear();
        for (var i = 0; i < columns.Length; i++)
        {
            var label = new Label
            {
                Text = columns[i],
                FontAttributes = FontAttributes.Bold,
                FontSize = 12,
                TextColor = Application.Current!.RequestedTheme == AppTheme.Dark
                    ? Color.FromArgb("#B8A4F0")
                    : Color.FromArgb("#5E35B1")
            };
            HeaderGrid.Add(label, i);
        }
    }

    static ColumnDefinitionCollection CreateColumns(int count)
    {
        var defs = new ColumnDefinitionCollection();
        for (var i = 0; i < count; i++)
            defs.Add(new ColumnDefinition(GridLength.Star));
        return defs;
    }

    static Label CreateLabel(string bindingPath, bool isMuted = false)
    {
        var label = new Label
        {
            FontSize = 13,
            LineBreakMode = LineBreakMode.TailTruncation,
            TextColor = isMuted
                ? Color.FromArgb("#999999")
                : null
        };
        label.SetBinding(Label.TextProperty, bindingPath);
        return label;
    }
}
