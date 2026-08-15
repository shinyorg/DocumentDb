using Shiny.DocumentDb;
using Shiny.DocumentDb.Geofencing;

namespace Sample.Maui;

public partial class MainPage : ContentPage
{
    readonly IDocumentStore store;
    readonly IDocumentGeofenceManager geofences;
    readonly GeofenceLog geofenceLog;
    string selectedType = "Customer";

    public MainPage(IDocumentStore store, IDocumentGeofenceManager geofences, GeofenceLog geofenceLog)
    {
        InitializeComponent();
        this.store = store;
        this.geofences = geofences;
        this.geofenceLog = geofenceLog;
        ResultsView.SelectionChanged += OnResultSelected;
        this.geofenceLog.Changed += OnGeofenceLogChanged;
        _ = InitializeAsync();
    }

    async Task InitializeAsync()
    {
        try
        {
            await SeedData.SeedAsync(store);
            await SeedData.SeedVectorsAsync(store);
            await SeedData.SeedGeofencesAsync(store);
            await LoadCustomersAsync();
        }
        catch (Exception ex)
        {
            // Surface startup/seed failures instead of silently swallowing them.
            StatusLabel.Text = ex.Message;
        }
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

    void OnVectorsClicked(object? sender, EventArgs e)
    {
        selectedType = "Vector";
        UpdateTabStyles();
        _ = LoadVectorNotesAsync();
    }

    void OnGeofencesClicked(object? sender, EventArgs e)
    {
        selectedType = "Geofence";
        UpdateTabStyles();
        UpdateMonitorButton();
        DisplayGeofenceLog();
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

        StyleTab(BtnCustomers, selectedType == "Customer");
        StyleTab(BtnOrders, selectedType == "Order");
        StyleTab(BtnVectors, selectedType == "Vector");
        StyleTab(BtnGeofences, selectedType == "Geofence");

        // The WHERE box means nothing on the geofence tab - swap in its own controls.
        QueryBar.IsVisible = selectedType != "Geofence";
        GeofenceBar.IsVisible = selectedType == "Geofence";

        void StyleTab(Button button, bool active)
        {
            button.BackgroundColor = active ? primary : inactiveBackground;
            button.TextColor = active ? Colors.White : inactiveText;
        }
    }

    async void OnQueryExecuted(object? sender, EventArgs e)
    {
        // The Vectors tab is driven by tapping a note, not the WHERE box.
        if (selectedType == "Vector")
        {
            await LoadVectorNotesAsync();
            return;
        }

        // The Geofences tab has its own controls.
        if (selectedType == "Geofence")
            return;

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

    async Task LoadVectorNotesAsync()
    {
        if (!store.SupportsVector)
        {
            CountLabel.Text = "0 notes";
            StatusLabel.Text = "Vector search is not available on this build.";
            ResultsView.ItemsSource = null;
            return;
        }
        var notes = await store.Query<VectorNote>().ToList();
        DisplayVectorNotes(notes);
    }

    // Tapping a note runs a nearest-neighbour search using that note's embedding as the query.
    async void OnResultSelected(object? sender, SelectionChangedEventArgs e)
    {
        if (selectedType != "Vector")
            return;
        if (e.CurrentSelection.FirstOrDefault() is not VectorNote note)
            return;

        ResultsView.SelectedItem = null; // clear highlight (fires again with empty selection → ignored)
        try
        {
            var hits = await store.NearestVectors<VectorNote>(note.Embedding, k: 5);
            DisplayVectorResults(note, hits);
        }
        catch (Exception ex)
        {
            StatusLabel.Text = ex.Message;
        }
    }

    void DisplayVectorNotes(IReadOnlyList<VectorNote> notes)
    {
        StatusLabel.Text = "Tap a note to find its nearest vectors.";
        ResultsView.SelectionMode = SelectionMode.Single;
        CountLabel.Text = $"{notes.Count} notes";
        SetHeader("Title", "Category");
        ResultsView.ItemTemplate = new DataTemplate(() =>
        {
            var grid = new Grid
            {
                ColumnDefinitions = CreateColumns(2),
                Padding = new Thickness(12, 8)
            };
            grid.Add(CreateLabel("Title"), 0);
            grid.Add(CreateLabel("Category", true), 1);
            return grid;
        });
        ResultsView.ItemsSource = notes;
    }

    void DisplayVectorResults(VectorNote query, IReadOnlyList<VectorResult<VectorNote>> hits)
    {
        StatusLabel.Text = "Tap the Vectors tab to pick another note.";
        ResultsView.SelectionMode = SelectionMode.None;
        CountLabel.Text = $"nearest to “{query.Title}”";
        SetHeader("Title", "Category", "Distance");
        ResultsView.ItemTemplate = new DataTemplate(() =>
        {
            var grid = new Grid
            {
                ColumnDefinitions = CreateColumns(3),
                Padding = new Thickness(12, 8)
            };
            grid.Add(CreateLabel("Document.Title"), 0);
            grid.Add(CreateLabel("Document.Category", true), 1);
            grid.Add(CreateLabel("Score", false, "{0:F4}"), 2);
            return grid;
        });
        ResultsView.ItemsSource = hits;
    }

    void DisplayCustomers(IReadOnlyList<Customer> customers)
    {
        StatusLabel.Text = "";
        ResultsView.SelectionMode = SelectionMode.None;
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
        StatusLabel.Text = "";
        ResultsView.SelectionMode = SelectionMode.None;
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

    // ── Geofencing ──────────────────────────────────────────────────────────────────────────────

    async void OnToggleMonitoringClicked(object? sender, EventArgs e)
    {
        try
        {
            StatusLabel.Text = "";
            if (geofences.IsStarted)
            {
                await geofences.Stop();
            }
            else
            {
                // Background location has to be granted before the listener will produce readings.
                var access = await geofences.RequestAccess();
                if (access != AccessState.Available)
                {
                    StatusLabel.Text = $"Location access: {access}";
                    return;
                }
                await geofences.Start();
            }
            UpdateMonitorButton();
        }
        catch (Exception ex)
        {
            StatusLabel.Text = ex.Message;
        }
    }


    async void OnWhereAmIClicked(object? sender, EventArgs e)
    {
        try
        {
            // One query per region set against the last GPS reading - no events, no state change.
            var current = await geofences.GetCurrent();
            StatusLabel.Text = current.Count == 0
                ? "No GPS reading yet - start monitoring first."
                : String.Join("   ", current.Select(c => $"{c.RegionSet}: {c.RegionName ?? "outside"}"));
        }
        catch (Exception ex)
        {
            StatusLabel.Text = ex.Message;
        }
    }


    async void OnZonesClicked(object? sender, EventArgs e)
    {
        try
        {
            var zones = await store.Query<GeofenceZone>().ToList();
            StatusLabel.Text = "Regions are ordinary documents - these back the \"zones\" set.";
            ResultsView.SelectionMode = SelectionMode.None;
            CountLabel.Text = $"{zones.Count} zones";
            SetHeader("Id", "Name", "City", "Active");
            ResultsView.ItemTemplate = new DataTemplate(() =>
            {
                var grid = new Grid
                {
                    ColumnDefinitions = CreateColumns(4),
                    Padding = new Thickness(12, 8)
                };
                grid.Add(CreateLabel("Id", true), 0);
                grid.Add(CreateLabel("Name"), 1);
                grid.Add(CreateLabel("City"), 2);
                grid.Add(CreateLabel("Active", true), 3);
                return grid;
            });
            ResultsView.ItemsSource = zones;
        }
        catch (Exception ex)
        {
            StatusLabel.Text = ex.Message;
        }
    }


    // The delegate fires on a background thread, so marshal before touching the UI.
    void OnGeofenceLogChanged(object? sender, EventArgs e)
        => MainThread.BeginInvokeOnMainThread(() =>
        {
            if (selectedType == "Geofence")
                DisplayGeofenceLog();
        });


    void DisplayGeofenceLog()
    {
        var entries = geofenceLog.Entries;
        StatusLabel.Text = entries.Count == 0
            ? "Start monitoring, then move between regions - transitions land here."
            : "";
        ResultsView.SelectionMode = SelectionMode.None;
        CountLabel.Text = $"{entries.Count} events";
        SetHeader("Time", "Event", "Set", "Region", "City");
        ResultsView.ItemTemplate = new DataTemplate(() =>
        {
            var grid = new Grid
            {
                ColumnDefinitions = CreateColumns(5),
                Padding = new Thickness(12, 8)
            };
            grid.Add(CreateLabel("Time", true), 0);
            grid.Add(CreateLabel("Verb"), 1);
            grid.Add(CreateLabel("RegionSet", true), 2);
            grid.Add(CreateLabel("Region"), 3);
            grid.Add(CreateLabel("City", true), 4);
            return grid;
        });
        ResultsView.ItemsSource = entries;
    }


    void UpdateMonitorButton()
        => BtnMonitor.Text = geofences.IsStarted ? "Stop Monitoring" : "Start Monitoring";


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

    static Label CreateLabel(string bindingPath, bool isMuted = false, string? stringFormat = null)
    {
        var label = new Label
        {
            FontSize = 13,
            LineBreakMode = LineBreakMode.TailTruncation,
            TextColor = isMuted
                ? Color.FromArgb("#999999")
                : null
        };
        label.SetBinding(Label.TextProperty, bindingPath, stringFormat: stringFormat);
        return label;
    }
}
