package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vaadin.componentfactory.Autocomplete;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.Html;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.checkbox.Checkbox;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.html.Image;
import com.vaadin.flow.component.icon.Icon;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.orderedlayout.*;
import com.vaadin.flow.component.textfield.IntegerField;
import com.vaadin.flow.data.renderer.ComponentRenderer;
import com.vaadin.flow.internal.Pair;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.server.StreamResource;
import com.vaadin.flow.theme.lumo.LumoUtility;
import dk.aau.cs.dkwe.edao.connector.DataLakeService;
import dk.aau.cs.dkwe.edao.jazero.communication.Response;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.User;
import dk.aau.cs.dkwe.edao.jazero.web.Main;
import dk.aau.cs.dkwe.edao.jazero.web.util.ConfigReader;
import dk.aau.cs.dkwe.edao.structures.Query;
import dk.aau.cs.dkwe.edao.structures.TableQuery;
import org.springframework.web.servlet.View;

import java.io.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Route(value = "")
public class SearchView extends Div
{
    private final VerticalLayout layout = new VerticalLayout();
    private final List<List<StringBuilder>> query = new ArrayList<>();
    private final Section querySection = new Section();
    private final Grid<Pair<String, Integer>> entityCounts = new Grid<>();
    private final Main main;
    private final View error;
    private Result result;
    private Component searchComponent;
    private Component resultComponent = null;
    private Component statsComponent;
    private String dataLake = null;
    private Map<String, Integer> stats = null;
    private static final boolean debug = true;
    private static final Random random = new Random();

    public SearchView(View error, Main main)
    {
        this.layout.setDefaultHorizontalComponentAlignment(FlexComponent.Alignment.CENTER);

        Component header = buildHeader(), selectDL = buildSelectDataLake(), searchBar = buildSearchBar();
        this.searchComponent = searchBar;
        this.searchComponent.setVisible(false);
        this.error = error;
        this.entityCounts.setHeight("200px");

        Div mainPage = new Div(selectDL, searchBar);
        this.layout.add(header, mainPage);
        add(this.layout);
        setHeightFull();
        this.main = main;
    }

    private void loadDLStatistics(String ip, User user)
    {
        this.stats = new HashMap<>();

        if (debug)
        {
            int entities = Math.abs(random.nextInt()) % 10000000;
            this.stats.put("Entities", entities);
            this.stats.put("Types", (int) Math.ceil(entities * 3.8));
            this.stats.put("Predicates", (int) Math.ceil(entities * 5.5));
            this.stats.put("Embeddings", (int) Math.ceil(entities * 0.8));
            this.stats.put("Linked cells", entities);
            this.stats.put("Tables", (int) Math.ceil((double) entities / 175));
            return;
        }

        try
        {
            DataLakeService dl = new DataLakeService(ip, user);
            Response response = dl.stats();

            if (response.getResponseCode() != 200)
            {
                this.stats = null;
                return;
            }

            JsonObject json = JsonParser.parseString((String) response.getResponse()).getAsJsonObject();
            this.stats.put("Entities", json.get("entities").getAsInt());
            this.stats.put("Types", json.get("types").getAsInt());
            this.stats.put("Predicates", json.get("predicates").getAsInt());
            this.stats.put("Embeddings", json.get("embeddings").getAsInt());
            this.stats.put("Linked cells", json.get("linked cells").getAsInt());
            this.stats.put("Tables", json.get("tables").getAsInt());
        }

        catch (Exception ignored) {}
    }

    private Component buildHeader()
    {
        HorizontalLayout header = new HorizontalLayout();
        header.setWidthFull();
        header.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        header.setAlignItems(FlexComponent.Alignment.CENTER);
        header.getStyle().set("padding", "5px");
        header.getStyle().set("border-bottom", "1px solid #ccc");
        header.getStyle().set("background-size", "cover");
        header.getStyle().set("height", "150px");

        Image logo = new Image("images/logo.png", "Jazero Logo");
        logo.setHeight("120px");

        Div systemName = new Div();
        systemName.setText("Jazero");
        systemName.getStyle().set("font-size", "100px");
        systemName.getStyle().set("font-weight", "bold");
        systemName.getStyle().set("margin-left", "10px");
        header.add(logo, systemName);

        return header;
    }

    private Component buildSelectDataLake()
    {
        VerticalLayout layout = new VerticalLayout();
        H2 label = new H2("Select Data Lake");
        ComboBox<String> dataLakes = new ComboBox<>("Data lake");
        dataLakes.setItems(ConfigReader.dataLakes());
        dataLakes.setRenderer(new ComponentRenderer<>(item -> {
            Span span = new Span(item);
            span.addClassNames("drop-down-items");
            return span;
        }));
        dataLakes.setClassName("combo-box");
        dataLakes.addValueChangeListener(event -> {
            this.dataLake = dataLakes.getValue();
            this.searchComponent.setVisible(true);
            String ip = ConfigReader.getIp(this.dataLake),
                    username = ConfigReader.getUsername(this.dataLake),
                    password = ConfigReader.getPassword(this.dataLake);
            loadDLStatistics(ip, new User(username, password, true));
            this.statsComponent.setVisible(true);
        });
        this.statsComponent = new Button(new Icon(VaadinIcon.INFO_CIRCLE), statsEvent -> {
            Dialog stats = new Dialog("Statistics");
            VerticalLayout statsLayout = new VerticalLayout();

            for (Map.Entry<String, Integer> stat : this.stats.entrySet())
            {
                HorizontalLayout statLayout = new HorizontalLayout();
                H4 text = new H4(stat.getKey() + ": " + stat.getValue());
                statLayout.add(text);
                statsLayout.add(statLayout);
            }

            Button closeButton = new Button(new Icon("lumo", "cross"), buttonEvent -> stats.close());
            statsLayout.setAlignItems(FlexComponent.Alignment.END);
            statsLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.END);
            stats.add(statsLayout);
            stats.getHeader().add(closeButton);
            stats.open();
        });
        this.statsComponent.setVisible(false);
        layout.add(label, dataLakes, this.statsComponent);
        layout.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.AlignContent.CENTER,
                LumoUtility.JustifyContent.CENTER);

        return layout;
    }

    private Component buildSearchBar()
    {
        VerticalLayout layout = new VerticalLayout();
        Component queryInput = buildQueryInput();
        Component entityCounts = buildEntityCounts();
        Component actionComponent = buildSearchComponent();
        layout.add(entityCounts, queryInput, actionComponent);
        layout.setAlignItems(FlexComponent.Alignment.CENTER);
        layout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);

        return layout;
    }

    private Component buildQueryInput()
    {
        FlexLayout tableLayout = new FlexLayout();
        HorizontalLayout header = new HorizontalLayout();
        H2 label = new H2("Input Query");
        label.addClassNames(LumoUtility.FontWeight.BOLD);
        initializeQueryTable();
        buildQueryTable();

        Scroller queryScroller = new Scroller(new Div(this.querySection));
        Icon addRowIcon = new Icon(VaadinIcon.CHEVRON_DOWN), removeRowIcon = new Icon(VaadinIcon.CHEVRON_UP),
                addColumnIcon = new Icon(VaadinIcon.CHEVRON_RIGHT), removeColumnIcon = new Icon(VaadinIcon.CHEVRON_LEFT);
        addRowIcon.setColor("#3D423F");
        removeRowIcon.setColor("#3D423F");
        addColumnIcon.setColor("#3D423F");
        removeColumnIcon.setColor("#3D423F");
        queryScroller.setMaxWidth("1200px");
        queryScroller.setMinWidth("630px");
        queryScroller.setMaxHeight("350px");

        Button addRowButton = new Button(new HorizontalLayout(addRowIcon, new H4("Add row")), item -> addRow());
        Button removeRowButton = new Button(new HorizontalLayout(removeRowIcon, new H4("Remove row")), item -> removeRow());
        Button addColumnButton = new Button(new HorizontalLayout(addColumnIcon, new H4("Add column")), item -> addColumn());
        Button removeColumnButton = new Button(new HorizontalLayout(removeColumnIcon, new H4("Remove column")), item -> removeColumn());
        Button clearQueryButton = new Button("Clear query", event -> clearQueryTable());
        addRowButton.getStyle().set("background-color", "#D1D5D9");
        removeRowButton.getStyle().set("background-color", "#D1D5D9");
        addColumnButton.getStyle().set("background-color", "#D1D5D9");
        removeColumnButton.getStyle().set("background-color", "#D1D5D9");
        clearQueryButton.getStyle().set("margin-left", "100px");
        clearQueryButton.getStyle().set("background-color", "#FD7B7C");
        clearQueryButton.getStyle().set("--vaadin-button-text-color", "white");
        header.add(label, clearQueryButton);

        HorizontalLayout rowButtonsLayout = new HorizontalLayout(addRowButton, removeRowButton);
        VerticalLayout columnButtonsLayout = new VerticalLayout(addColumnButton, removeColumnButton);
        HorizontalLayout horizontalCell = new HorizontalLayout(queryScroller, columnButtonsLayout);
        VerticalLayout verticalCell = new VerticalLayout(header, horizontalCell, rowButtonsLayout);
        tableLayout.add(verticalCell);

        return tableLayout;
    }

    private Component buildEntityCounts()
    {
        H3 label = new H3("Entity counts");
        label.addClassNames(LumoUtility.FontWeight.BOLD);
        updateEntityCounts();

        return new VerticalLayout(label, this.entityCounts);
    }

    private Component buildSearchComponent()
    {
        VerticalLayout leftColumnLayout = new VerticalLayout();
        ComboBox<String> entitySimilarities = new ComboBox<>("Entity similarity");
        IntegerField topKField = new IntegerField("Top-K");
        entitySimilarities.setItems("RDF types", "Predicates", "Embeddings");
        entitySimilarities.setRenderer(new ComponentRenderer<>(item -> {
            Span span = new Span(item);
            span.addClassNames("drop-down-items");
            return span;
        }));
        entitySimilarities.setClassName("combo-box");
        topKField.setRequiredIndicatorVisible(true);
        topKField.setMin(1);
        topKField.setMax(500000);
        topKField.setValue(10);
        topKField.setStepButtonsVisible(true);
        leftColumnLayout.add(entitySimilarities, topKField);

        Checkbox prefilterBox = new Checkbox("Pre-filter", false);
        VerticalLayout rightColumnLayout = new VerticalLayout();
        Button searchButton = new Button("Search", event -> search(topKField.getValue(), entitySimilarities.getValue(),
                prefilterBox.getValue()));
        searchButton.setWidth("200px");
        searchButton.setHeight("80px");
        searchButton.getStyle().set("background-color", "#57AF34");
        searchButton.setClassName("search-button");
        rightColumnLayout.add(prefilterBox, searchButton);

        return new HorizontalLayout(leftColumnLayout, rightColumnLayout);
    }

    private void addRow()
    {
        int columns = !this.query.isEmpty() ? this.query.get(0).size() : 3;
        List<StringBuilder> newRow = new ArrayList<>(columns);

        for (int column = 0; column < columns; column++)
        {
            newRow.add(new StringBuilder());
        }

        this.query.add(newRow);
        buildQueryTable();
    }

    private void removeRow()
    {
        if (this.query.size() > 1)
        {
            this.query.remove(this.query.size() - 1);
            buildQueryTable();
        }
    }

    private void addColumn()
    {
        int rows = this.query.size();

        for (int row = 0; row < rows; row++)
        {
            this.query.get(row).add(new StringBuilder());
        }

        buildQueryTable();
    }

    private void removeColumn()
    {
        if (this.query.get(0).size() > 1)
        {
            int rows = this.query.size();

            for (int row = 0; row < rows; row++)
            {
                this.query.get(row).remove(this.query.get(row).size() - 1);
            }

            buildQueryTable();
        }
    }

    private void buildQueryTable()
    {
        VerticalLayout tableRowLayout = new VerticalLayout();
        int rows = this.query.size();

        for (int row = 0; row < rows; row++)
        {
            HorizontalLayout tableColumnLayout = new HorizontalLayout();
            int columns = this.query.get(row).size();

            for (int column = 0; column < columns; column++)
            {
                int rowCoordinate = row, columnCoordinate = column;
                Autocomplete textField = new Autocomplete();
                String cellContent = this.query.get(row).get(column).toString();
                textField.setValue(cellContent);
                textField.getStyle().set("font-family", "Courier New");
                textField.addChangeListener(event -> {
                    String content = event.getValue();
                    textField.setOptions(keywordSearch(content));
                });
                textField.addAutocompleteValueAppliedListener(event -> {
                    String content = event.getValue();
                    int oldContentLength = this.query.get(rowCoordinate).get(columnCoordinate).length();
                    this.query.get(rowCoordinate).get(columnCoordinate).replace(0, oldContentLength, content);
                    updateEntityCounts();
                });
                tableColumnLayout.add(textField);
            }

            tableRowLayout.add(tableColumnLayout);
        }

        this.querySection.removeAll();
        this.querySection.add(tableRowLayout);
    }

    private List<String> keywordSearch(String query)
    {
        String dataLakeIp = ConfigReader.getIp(this.dataLake),
                username = ConfigReader.getUsername(this.dataLake),
                password = ConfigReader.getPassword(this.dataLake);
        User user = new User(username, password, true);

        try
        {
            if (debug)
            {
                query = query.toLowerCase();
                TimeUnit.MILLISECONDS.sleep(300);

                if (query.startsWith("m"))
                {
                    return List.of("https://dbpedia.org/page/MERS", "https://dbpedia.org/page/Measles", "https://dbpedia.org/page/Malaria", "https://dbpedia.org/page/Middle_East");
                }

                else if (query.startsWith("z"))
                {
                    return List.of("https://dbpedia.org/page/Zanamivir");
                }

                else if (query.startsWith("o"))
                {
                    return List.of("https://dbpedia.org/page/Oseltamivir");
                }

                else if (query.startsWith("r"))
                {
                    return List.of("https://dbpedia.org/page/Rhinovirus", "https://dbpedia.org/page/Rabies", "https://dbpedia.org/page/Respiratory_system", "https://dbpedia.org/page/Respiratory_syncytial_virus");
                }

                else if (query.startsWith("b"))
                {
                    return List.of("https://dbpedia.org/page/Baloxavir", "https://dbpedia.org/page/Bird", "https://dbpedia.org/page/Bat", "https://dbpedia.org/page/Bronchitis");
                }

                else if (query.startsWith("c"))
                {
                    return List.of("https://dbpedia.org/page/Chimpanzee", "https://dbpedia.org/page/Cancer", "https://dbpedia.org/page/Cholera");
                }

                else if (query.startsWith("i"))
                {
                    return List.of("https://dbpedia.org/page/Influenza", "https://dbpedia.org/page/Italy", "https://dbpedia.org/page/India");
                }

                else if (query.startsWith("sa"))
                {
                    return List.of("https://dbpedia.org/page/SARS");
                }

                else if (query.startsWith("sw"))
                {
                    return List.of("https://dbpedia.org/page/Switzerland");
                }

                else if (query.startsWith("un"))
                {
                    return List.of("https://dbpedia.org/page/University_of_Basel");
                }

                else if (query.startsWith("u"))
                {
                    return List.of("https://dbpedia.org/page/USA");
                }

                else if (query.startsWith("ad"))
                {
                    return List.of("https://dbpedia.org/page/Adenovirus");
                }

                else if (query.startsWith("a"))
                {
                    return List.of("https://dbpedia.org/page/AOU");
                }

                else if (query.startsWith("p"))
                {
                    return List.of("https://dbpedia.org/page/Peramivir");
                }

                else if (query.startsWith("g"))
                {
                    return List.of("https://dbpedia.org/page/GlaxoSmithKline");
                }

                return List.of();
            }

            DataLakeService dl = new DataLakeService(dataLakeIp, user);
            Response res = dl.keywordSearch(query);

            if (res.getResponseCode() != 200)
            {
                return new ArrayList<>();
            }

            return (List<String>) res.getResponse();
        }

        catch (Exception e)
        {
            return new ArrayList<>();
        }
    }

    private void initializeQueryTable()
    {
        int rows = 3, columns = 3;

        for (int i = 0; i < rows; i++)
        {
            List<StringBuilder> column = new ArrayList<>();

            for (int j = 0; j < columns; j++)
            {
                column.add(new StringBuilder());
            }

            this.query.add(column);
        }
    }

    private void clearQueryTable()
    {
        this.query.clear();
        updateEntityCounts();
        initializeQueryTable();
        buildQueryTable();
    }

    private void setQuery(Table<String> table)
    {
        int rows = table.rowCount();
        this.query.clear();

        for (int row = 0; row < rows; row++)
        {
            Table.Row<String> tableRow = table.getRow(row);
            int columns = tableRow.size();
            List<StringBuilder> columnItems = new ArrayList<>();

            for (int column = 0; column < columns; column++)
            {
                String cell = table.getRow(row).get(column);
                List<String> links = keywordSearch(cell);

                if (!links.isEmpty())
                {
                    columnItems.add(new StringBuilder(links.get(0)));
                }

                else
                {
                    columnItems.add(new StringBuilder());
                }
            }

            if (!columnItems.isEmpty())
            {
                this.query.add(columnItems);
            }
        }

        if (!this.query.isEmpty())
        {
            for (int i = 0; i < this.query.get(0).size(); i++)
            {
                int column = i;

                if (this.query.stream().allMatch(row -> row.get(column).isEmpty()))
                {
                    this.query.forEach(row -> row.remove(column));
                }
            }
        }

        updateEntityCounts();
        buildQueryTable();
    }

    private void updateEntityCounts()
    {
        this.entityCounts.removeAllColumns();
        this.entityCounts.addComponentColumn(item -> {
            VerticalLayout layout = new VerticalLayout();
            Html entity = new Html("<div><b>Entity</b>" + item.getFirst() + "</div>"),
                    count = new Html("<div><b>Count</b>" + item.getSecond() + "</div>");
            layout.add(entity, count);

            return layout;
        }).setHeader("Entity counts").setVisible(false);

        try
        {
            this.entityCounts.addColumn(Pair::getFirst).setHeader("Entity");
            this.entityCounts.addColumn(Pair::getSecond).setHeader("Count");
            this.entityCounts.setItems(tableContents());
            this.entityCounts.setWidth("600px");
        }

        catch (RuntimeException e)
        {
            Dialog errorDialog = errorDialog(e.getMessage());
            errorDialog.open();
        }
    }

    private Set<Pair<String, Integer>> tableContents()
    {
        Set<Pair<String, Integer>> contents = new TreeSet<>(Comparator.comparing(Pair::getFirst));

        for (List<StringBuilder> row : this.query)
        {
            for (StringBuilder cell : row)
            {
                String content = cell.toString();

                if (!content.isEmpty())
                {
                    contents.add(new Pair<>(content, count(content)));
                }
            }
        }

        return contents;
    }

    private Dialog errorDialog(String message)
    {
        Dialog errorDialog = new Dialog("Error");
        VerticalLayout layout = new VerticalLayout();
        H4 h4Message = new H4(message);
        layout.add(h4Message);
        errorDialog.add(layout);

        Button closeButton = new Button(new Icon("lumo", "cross"), event -> errorDialog.close());
        errorDialog.getHeader().add(closeButton);
        this.layout.add(errorDialog);

        return errorDialog;
    }

    private int count(String entity)
    {
        if (debug)
        {
            return Math.abs(random.nextInt() % 100000);
        }

        else if (this.dataLake == null)
        {
            Dialog errorDialog = errorDialog("Please select a data lake");
            errorDialog.open();

            return -1;
        }

        else if (debug)
        {
            return -1;
        }

        String dlHost = ConfigReader.getIp(this.dataLake), username = ConfigReader.getUsername(this.dataLake),
                password = ConfigReader.getPassword(this.dataLake);
        DataLakeService dl = new DataLakeService(dlHost, new User(username, password, true));
        return Integer.parseInt((String) dl.count(entity).getResponse());
    }

    private void search(int topK, String entitySimilarity, boolean prefilter)
    {
        if (topK <= 0 || entitySimilarity == null || dataLake == null)
        {
            return;
        }

        String dataLakeIp = ConfigReader.getIp(this.dataLake),
                username = ConfigReader.getUsername(this.dataLake),
                password = ConfigReader.getPassword(this.dataLake);
        User user = new User(username, password, true);
        TableSearch.EntitySimilarity similarity;
        Query query = parseQuery();

        if (entitySimilarity.equals("Embeddings"))
        {
            similarity = TableSearch.EntitySimilarity.EMBEDDINGS_ABS;
        }

        else if (entitySimilarity.equals("RDF types"))
        {
            similarity = TableSearch.EntitySimilarity.JACCARD_TYPES;
        }

        else if (entitySimilarity.equals("Predicates"))
        {
            similarity = TableSearch.EntitySimilarity.JACCARD_PREDICATES;
        }

        else
        {
            throw new RuntimeException("Not recognized: (" + entitySimilarity + ")");
        }

        try
        {
            if (debug)
            {
                this.result = parseDebugResult();
                refreshResults();
                return;
            }

            DataLakeService dl = new DataLakeService(dataLakeIp, user);
            Response pingResponse = dl.ping();

            if (pingResponse.getResponseCode() != 200)
            {
                throw new RuntimeException("Connection error");
            }

            this.result = dl.search(query, topK, similarity, prefilter);
            refreshResults();
        }

        catch (RuntimeException e)
        {
            Dialog errorDialog = errorDialog(e.getMessage());
            errorDialog.open();
        }
    }

    private Query parseQuery()
    {
        List<List<String>> queryAsList = this.query.stream().map(row -> row.stream()
                .map(StringBuilder::toString)
                .collect(Collectors.toList())).toList();
        Table<String> queryAsTable = new DynamicTable<>(queryAsList);

        return new TableQuery(queryAsTable);
    }

    private Result parseDebugResult()
    {
        try (BufferedReader reader = new BufferedReader(new FileReader("first_output.json")))
        {
            int c;
            StringBuilder builder = new StringBuilder();

            while ((c = reader.read()) != -1)
            {
                builder.append((char) c);
            }

            return Result.fromJson(builder.toString());
        }

        catch (IOException e)
        {
            return new Result(0, List.of(), -1.0, -1.0, Map.of());
        }
    }

    private void refreshResults()
    {
        clearResults();

        this.resultComponent = buildResults();
        this.layout.add(this.resultComponent);
    }

    private void clearResults()
    {
        if (this.resultComponent != null)
        {
            this.layout.remove(this.resultComponent);
            this.resultComponent = null;
        }
    }

    private Component buildResults()
    {
        VerticalLayout resultLayout = new VerticalLayout();
        HorizontalLayout resultHeader = new HorizontalLayout(), subHeader = new HorizontalLayout();
        Section resultSection = new Section();
        Component resultsList = buildResultsList();
        H1 resultLabel = new H1("Results");
        H2 topKLabel = new H2("Top-" + this.result.getSize());
        Dialog stats = statsDialog();
        Button clearButton = new Button("Clear", event -> clearResults()),
                statsButton = new Button("Statistics", event -> stats.open());
        Button downloadResultsButton =
                new Button(downloadContent("results.json", "Download result", this.result.toString().getBytes()));
        clearButton.getStyle().set("margin-left", "20px");
        clearButton.getStyle().set("background-color", "#FD7B7C");
        clearButton.getStyle().set("--vaadin-button-text-color", "white");
        statsButton.getStyle().set("background-color", "#00669E");
        statsButton.getStyle().set("--vaadin-button-text-color", "white");
        downloadResultsButton.getStyle().set("background-color", "#82EC9E");
        downloadResultsButton.getStyle().set("--vaadin-button-text-color", "white");
        resultHeader.add(resultLabel, clearButton);
        subHeader.add(statsButton, downloadResultsButton);
        resultLayout.add(resultHeader, subHeader, topKLabel, resultsList);
        resultLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        resultLayout.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.JustifyContent.CENTER, LumoUtility.Display.FLEX);
        clearResults();
        resultSection.add(resultLayout);

        Scroller scroller = new Scroller(new Div(resultSection));
        scroller.getStyle().set("border-top", "2px solid #ccc");
        scroller.setWidthFull();
        scroller.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.JustifyContent.CENTER, LumoUtility.Display.FLEX);

        return scroller;
    }

    private Dialog statsDialog()
    {
        Dialog dialog = new Dialog("Statistics");
        VerticalLayout layout = new VerticalLayout();
        H4 runtime = new H4("Runtime: " + this.result.getRuntime() / 1000000000 + "s"),
                reduction = new H4("Search space reduction: " + this.result.getReduction() * 100 + "%");
        layout.add(runtime, reduction);
        dialog.add(layout);

        Button closeButton = new Button(new Icon("lumo", "cross"), event -> dialog.close());
        dialog.getHeader().add(closeButton);

        return dialog;
    }

    private Component buildResultsList()
    {
        VerticalLayout layout = new VerticalLayout();

        for (var resultTable : this.result.getTables())
        {
            double score = resultTable.first();
            Table<String> table = resultTable.second();
            H3 tableIdLabel = new H3(table.getId().replace(".json", "") + " (score: " + score + ")");
            HorizontalLayout iconLayout = new HorizontalLayout(new Icon(VaadinIcon.ELLIPSIS_DOTS_V));
            iconLayout.setWidthFull();
            iconLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
            iconLayout.setAlignItems(FlexComponent.Alignment.CENTER);

            Div tableSnippet = new Div(tableSnippet(table), iconLayout);
            tableSnippet.addClickListener(event -> {
                Button downloadTableButton = new Button(downloadContent("table.txt", "Download table",
                        table.toStr().getBytes()));
                downloadTableButton.getStyle().set("background-color", "#82EC9E");
                downloadTableButton.getStyle().set("--vaadin-button-text-color", "white");
                downloadTableButton.getStyle().set("margin-left", "100px");

                HorizontalLayout header = new HorizontalLayout(new H2(table.getId()), downloadTableButton);
                Dialog resultDialog = new Dialog(header);
                VerticalLayout dialogLayout = new VerticalLayout(resultTableGrid(table));
                Button closeButton = new Button(new Icon("lumo", "cross"), buttonEvent -> resultDialog.close());
                resultDialog.add(dialogLayout);
                resultDialog.getHeader().add(closeButton);
                resultDialog.setWidth("4000px");
                resultDialog.open();
            });

            Button toQueryButton = new Button("Use as query", event -> setQuery(resultTable.second()));
            toQueryButton.getStyle().set("--vaadin-button-text-color", "white");
            toQueryButton.getStyle().set("background-color", "#636363");

            Button tableStatsButton = new Button(new Icon(VaadinIcon.INFO_CIRCLE), statsEvent -> {
                Dialog statsDialog = new Dialog("Table statistics");
                VerticalLayout statsLayout = new VerticalLayout();
                Map<String, Integer> stats = tableStats(table);

                for (Map.Entry<String, Integer> stat : stats.entrySet())
                {
                    H4 statContent = new H4(stat.getKey() + ": " + stat.getValue());
                    statsLayout.add(statContent);
                }

                Button closeButton = new Button(new Icon("lumo", "cross"), buttonEvent -> statsDialog.close());
                statsLayout.setAlignItems(FlexComponent.Alignment.END);
                statsLayout.setJustifyContentMode(FlexComponent.JustifyContentMode.END);
                statsDialog.add(statsLayout);
                statsDialog.getHeader().add(closeButton);
                statsDialog.open();
            });
            VerticalLayout tableLayout = new VerticalLayout(tableIdLabel, tableSnippet, new HorizontalLayout(toQueryButton, tableStatsButton));
            HorizontalLayout resultLayout = new HorizontalLayout(tableLayout);
            layout.add(resultLayout);
        }

        return layout;
    }

    private static Component tableSnippet(Table<String> table)
    {
        int rows = Integer.min(table.rowCount(), 2), columns = Integer.min(table.columnCount(), 3);
        Grid<List<String>> snippetGrid = new Grid<>();
        List<List<String>> snippetTable = new ArrayList<>();

        for (int row = 0; row < rows; row++)
        {
            List<String> tableRow = new ArrayList<>();

            for (int column = 0; column < columns; column++)
            {
                tableRow.add(table.getRow(row).get(column));
            }

            snippetTable.add(tableRow);
        }

        snippetGrid.setItems(snippetTable);

        for (int column = 0; column < columns; column++)
        {
            int i = column;
            snippetGrid.addColumn(row -> row.get(i)).setHeader(table.getColumnLabels()[i]);
        }

        snippetGrid.setHeight("130px");
        snippetGrid.setWidth("500px");

        return snippetGrid;
    }

    private static Component resultTableGrid(Table<String> table)
    {
        Grid<List<String>> grid = new Grid<>();
        grid.setItems(table.toList());

        for (int i = 0; i < table.getColumnLabels().length; i++)
        {
            int index = i;
            grid.addColumn(row -> row.get(index)).setHeader(table.getColumnLabels()[index]);
        }

        return grid;
    }

    private static Anchor downloadContent(String outputFile, String label, byte[] content)
    {
        StreamResource resource = new StreamResource(outputFile, () -> new ByteArrayInputStream(content));
        Anchor anchor = new Anchor(resource, label);
        anchor.getElement().setAttribute("download", true);

        return anchor;
    }

    private Map<String, Integer> tableStats(Table<String> table)
    {
        Map<String, Integer> stats = new HashMap<>();

        if (debug)
        {
            int entities = (int) Math.ceil(table.rowCount() * table.columnCount() * 0.277);
            stats.put("Entities", entities);
            stats.put("Types", 6869);
            stats.put("Predicates", 10050);
            stats.put("Embeddings", 32440283);
            stats.put("Table rows", table.rowCount());
            stats.put("Table columns", table.columnCount());

            return stats;
        }

        try
        {
            DataLakeService dl = new DataLakeService(null, null);
            Response response = dl.tableStats(table.getId());

            if (response.getResponseCode() != 200)
            {
                return new HashMap<>();
            }

            JsonObject json = JsonParser.parseString((String) response.getResponse()).getAsJsonObject();
            stats.put("Entities", json.get("entities").getAsInt());
            stats.put("Types", json.get("types").getAsInt());
            stats.put("Predicates", json.get("predicates").getAsInt());
            stats.put("Embeddings", json.get("embeddings").getAsInt());
            stats.put("Table rows", table.rowCount());
            stats.put("Table columns", table.columnCount());

            return stats;
        }

        catch (Exception e)
        {
            return stats;
        }
    }
}
