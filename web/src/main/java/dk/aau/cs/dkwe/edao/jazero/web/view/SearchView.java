package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.button.ButtonVariant;
import com.vaadin.flow.component.card.Card;
import com.vaadin.flow.component.checkbox.Checkbox;
import com.vaadin.flow.component.combobox.ComboBox;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.grid.Grid;
import com.vaadin.flow.component.html.*;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.component.notification.Notification;
import com.vaadin.flow.component.orderedlayout.FlexComponent;
import com.vaadin.flow.component.orderedlayout.HorizontalLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.component.textfield.IntegerField;
import com.vaadin.flow.router.Route;
import com.vaadin.flow.spring.annotation.VaadinSessionScope;
import com.vaadin.flow.theme.lumo.LumoUtility;
import dk.aau.cs.dkwe.edao.connector.DataLake;
import dk.aau.cs.dkwe.edao.connector.DataLakeService;
import dk.aau.cs.dkwe.edao.jazero.communication.Response;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.Result;
import dk.aau.cs.dkwe.edao.jazero.datalake.search.TableSearch;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.Pair;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.DynamicTable;
import dk.aau.cs.dkwe.edao.jazero.datalake.structures.table.Table;
import dk.aau.cs.dkwe.edao.jazero.datalake.system.User;
import dk.aau.cs.dkwe.edao.jazero.web.Main;
import dk.aau.cs.dkwe.edao.jazero.web.connector.MockDataLakeService;
import dk.aau.cs.dkwe.edao.jazero.web.util.ConfigReader;
import dk.aau.cs.dkwe.edao.structures.Query;
import dk.aau.cs.dkwe.edao.structures.TableQuery;
import org.springframework.web.servlet.View;

import java.util.*;

@Route(value = "")
@VaadinSessionScope
public class SearchView extends Div
{
    private static final boolean DEBUG = false;
    private DataLake dl;
    private String dataLake = null, dataLakeIp;
    private User user = null;
    private final VerticalLayout root = new VerticalLayout();
    private final ComboBox<String> dataLakeSelect = new ComboBox<>();

    private EditableQueryTable queryTable = null;
    private final Div enteredValuesList = new Div();

    private final ComboBox<String> similaritySelect = new ComboBox<>("Similarity function");
    private final IntegerField topKField = new IntegerField("Top-K");
    private final Checkbox prefilterCheckbox = new Checkbox("Prefilter search space");

    private final Button searchButton = new Button("Search", VaadinIcon.SEARCH.create());
    private final Button clearResultsButton = new Button("Clear results");
    private final Button searchStatsButton = new Button("Show search statistics", VaadinIcon.CHART.create());

    private final Grid<Pair<Double, Table<String>>> resultsGrid = new Grid<>();
    private Result currentResult;

    public SearchView(View error, Main main)
    {
        this.root.setVisible(false);
        this.root.setSizeFull();
        this.root.setPadding(true);
        this.root.setSpacing(true);
        this.root.setAlignItems(FlexComponent.Alignment.STRETCH);
        add(buildHeader());

        this.searchButton.addThemeVariants(ButtonVariant.LUMO_PRIMARY);
        this.searchButton.addClassName("search-button");
        this.clearResultsButton.addThemeVariants(ButtonVariant.LUMO_ERROR);
        this.searchStatsButton.addThemeVariants(ButtonVariant.LUMO_CONTRAST);

        this.queryTable = new EditableQueryTable((event, textField) -> {
            String content = event.getValue();
            textField.setOptions(keywordSearch(content));
        }, (event, textField) -> renderEnteredValues());

        // Data lake selection
        VerticalLayout selectDataLakeLayout = new VerticalLayout();
        Button dataLakeStats = new Button(VaadinIcon.CHART.create(), event -> {
            Response response = this.dl.stats();

            if (response.getResponseCode() != 200)
            {
                Notification.show("Could not connect to data lake and retrieve statistics", 2500, Notification.Position.TOP_CENTER);
                return;
            }

            String statsJson = (String) response.getResponse();
            new DataLakeStatsDialog(this.dataLake + " Statistics", parseStatistics(statsJson)).open();
        });
        Set<String> dataLakes = ConfigReader.dataLakes();
        dataLakeStats.setVisible(false);
        this.dataLakeSelect.setItems(dataLakes);
        this.dataLakeSelect.addValueChangeListener(e -> {
            this.dataLake = e.getValue();
            this.dataLakeIp = ConfigReader.getIp(this.dataLake);
            this.user = new User(ConfigReader.getUsername(this.dataLake), ConfigReader.getPassword(this.dataLake), false);
            this.dl = DEBUG ? new MockDataLakeService() : new DataLakeService(this.dataLakeIp, this.user);

            if (isConnected())
            {
                Notification.show("Connected to '" + e.getValue() + "'", 3000, Notification.Position.TOP_CENTER);
                this.root.setVisible(true);
                dataLakeStats.setVisible(true);
                dataLakeStats.setText(this.dataLake + " Statistics");
            }

            else
            {
                Notification.show("Couldn't connect to '" + e.getValue() + "'", 3000, Notification.Position.TOP_CENTER);
            }
        });
        this.dataLakeSelect.setPlaceholder("Select Data Lake");
        selectDataLakeLayout.add(new H2("Select Data Lake"), this.dataLakeSelect, dataLakeStats);
        selectDataLakeLayout.addClassNames(LumoUtility.AlignItems.CENTER, LumoUtility.AlignContent.CENTER,
                LumoUtility.JustifyContent.CENTER);
        add(selectDataLakeLayout);

        // Query table
        HorizontalLayout enteredHeader = new HorizontalLayout(new H4("Entity Frequency in Tables"));
        Card queryCard = new Card(), entityCountsCard = new Card();
        queryCard.addClassName("glass-card");
        entityCountsCard.addClassName("glass-card");
        queryCard.add(new H4("Query Table"), this.queryTable);
        entityCountsCard.add(enteredHeader, this.enteredValuesList);
        this.root.add(queryCard, entityCountsCard);

        // Parameters
        this.similaritySelect.setItems("RDF Types", "RDF Predicates", "Embeddings");
        this.similaritySelect.setPlaceholder("Select similarity");
        this.similaritySelect.setValue("RDF Types");
        this.topKField.setMin(1);
        this.topKField.setStep(1);
        this.topKField.setValue(10);
        this.topKField.setStepButtonsVisible(true);
        this.prefilterCheckbox.setValue(false);

        // Search controls
        HorizontalLayout searchControls = new HorizontalLayout(this.clearResultsButton, this.searchStatsButton);
        this.clearResultsButton.addClickListener(e -> clearResults());
        this.searchStatsButton.addClickListener(e -> {
            if (this.dataLake == null)
            {
                Notification.show("Select a data lake first", 2500, Notification.Position.TOP_CENTER);
                return;
            }

            new SearchStatsDialog(this.currentResult.getReduction() * 100, this.currentResult.getRuntime()).open();
        });

        // Results grid
        Card resultsCard = new Card();
        resultsCard.addClassName("glass-card");
        this.resultsGrid.addClassName("results-grid");
        configureResultsGrid();
        resultsCard.add(new H4("Results"), searchControls, this.resultsGrid);
        resultsCard.setVisible(false);

        FormLayout paramsLayout = new FormLayout(this.similaritySelect, this.topKField, this.prefilterCheckbox, this.searchButton);
        paramsLayout.setResponsiveSteps(new FormLayout.ResponsiveStep("0", 1),
                new FormLayout.ResponsiveStep("500px", 4));

        Card parameterCard = new Card();
        parameterCard.addClassName("glass-card");
        this.searchButton.addClickListener(e -> {
            executeSearch();
            resultsCard.setVisible(true);
        });
        parameterCard.add(new H4("Parameters"), paramsLayout);
        this.root.add(parameterCard, resultsCard);
        add(this.root);
    }

    private boolean isConnected()
    {
        return this.dl.ping().getResponseCode() < 300;
    }

    private Query parseQuery()
    {
        Table<String> queryAsTable = new DynamicTable<>(this.queryTable.generateModelValue());
        return new TableQuery(queryAsTable);
    }

    private static Map<String, Integer> parseStatistics(String jsonStr)
    {
        Map<String, Integer> stats = new HashMap<>();
        JsonObject json = JsonParser.parseString(jsonStr).getAsJsonObject();
        stats.put("Entities", json.get("entities").getAsInt());
        stats.put("Types", json.get("types").getAsInt());
        stats.put("Predicates", json.get("predicates").getAsInt());
        stats.put("Embeddings", json.get("embeddings").getAsInt());
        stats.put("Linked cells", json.get("linked cells").getAsInt());
        stats.put("Tables", json.get("tables").getAsInt());

        return stats;
    }

    private static Component buildHeader()
    {
        HorizontalLayout header = new HorizontalLayout();
        header.setWidthFull();
        header.setJustifyContentMode(FlexComponent.JustifyContentMode.CENTER);
        header.setAlignItems(FlexComponent.Alignment.CENTER);
        header.getStyle().set("padding", "5px");
        header.getStyle().set("border-bottom", "1px solid #717378");
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

    private void renderEnteredValues()
    {
        List<List<String>> queryTable = this.queryTable.generateModelValue();
        Set<Map.Entry<String, String>> nonEmpty = new HashSet<>();
        Set<String> seenCells = new HashSet<>();
        Div list = new Div();
        this.enteredValuesList.removeAll();

        if (queryTable == null)
        {
            return;
        }

        for (var row : queryTable)
        {
            for (var cell : row)
            {
                if (cell != null && !cell.isBlank() && !seenCells.contains(cell))
                {
                    Response countResponse = this.dl.count(cell);
                    seenCells.add(cell);

                    if (countResponse.getResponseCode() == 200)
                    {
                        nonEmpty.add(Map.entry(cell.replace(" ", ""), (String) countResponse.getResponse()));
                    }
                }
            }
        }

        if (nonEmpty.isEmpty())
        {
            this.enteredValuesList.add(new Div(new Div(new H4("No values entered."))));
            return;
        }

        for (var entry : nonEmpty)
        {
            list.add(new Div(new H5(entry.getKey() + " — " + entry.getValue())));
        }

        this.enteredValuesList.add(list);
    }

    private void executeSearch()
    {
        if (this.dataLake == null)
        {
            Notification.show("Select a data lake first", 2500, Notification.Position.TOP_CENTER);
            return;
        }
        if (this.similaritySelect.getValue() == null)
        {
            Notification.show("Select a similarity function", 2500, Notification.Position.TOP_CENTER);
            return;
        }

        Integer k = this.topKField.getValue();

        if (k == null || k < 1)
        {
            Notification.show("Top-K must be >= 1", 2500, Notification.Position.TOP_CENTER);
            return;
        }

        Query query = parseQuery();
        boolean prefilter = this.prefilterCheckbox.getValue();
        TableSearch.EntitySimilarity entitySimilarity;

        if (this.similaritySelect.getValue().toLowerCase().contains("types"))
        {
            entitySimilarity = TableSearch.EntitySimilarity.JACCARD_TYPES;
        }

        else if (this.similaritySelect.getValue().toLowerCase().contains("predicates"))
        {
            entitySimilarity = TableSearch.EntitySimilarity.JACCARD_PREDICATES;
        }

        else
        {
            entitySimilarity = TableSearch.EntitySimilarity.EMBEDDINGS_ABS;
        }

        this.currentResult = this.dl.search(query, k, entitySimilarity, prefilter);
        this.resultsGrid.setItems(this.currentResult.getTables());
    }

    private List<String> keywordSearch(String query)
    {
        Response res = this.dl.keywordSearch(query);

        if (res.getResponseCode() != 200)
        {
            return new ArrayList<>();
        }

        List<String> entities = (List<String>) res.getResponse();
        entities = entities.stream().map(entity -> entity.replaceAll("\\s+", "").replace("\"", "")).toList();

        return entities;
    }

    private void clearResults()
    {
        this.currentResult = null;
        this.resultsGrid.getDataProvider().refreshAll();
    }

    private void configureResultsGrid()
    {
        this.resultsGrid.addColumn(v -> v.second().getId()).setHeader("Table Name").setAutoWidth(true).setFlexGrow(1);
        this.resultsGrid.addColumn(pair -> String.format("%.2f", pair.first())).setHeader("Relevance").setAutoWidth(true).setFlexGrow(0);
        this.resultsGrid.addComponentColumn(rt -> {
            // Snippet table as a small grid
            Grid<List<String>> snippetGrid = new Grid<>();
            int maxCols = Math.min(rt.second().columnCount(), 3);

            for (int c = 0; c < maxCols; c++)
            {
                final int idx = c;
                snippetGrid.addColumn(row -> idx < row.size() ? row.get(idx) : "").setHeader("Col " + (c+1));
            }

            List<List<String>> resultTable = rt.second().toList();
            snippetGrid.setItems(resultTable.subList(0, Math.min(3, resultTable.size())));
            snippetGrid.setAllRowsVisible(true);
            snippetGrid.addItemClickListener(ev -> {
                // Clicking a snippet row opens full table dialog
                new TablePreviewDialog(rt.second().getId(), rt.second().toList()).open();
            });

            return snippetGrid;
        }).setHeader("Table Snippet").setFlexGrow(2);

        this.resultsGrid.addComponentColumn(rt -> {
            Button useAsQuery = new Button("Use as query", VaadinIcon.UPLOAD.create());
            useAsQuery.addClickListener(e -> setQueryTable(rt.second()));

            Button showStats = new Button("Show table statistics", VaadinIcon.CHART.create());
            showStats.addClickListener(e -> {
                Map<String, Integer> stats = this.dataLake != null ? tableStats(rt.second()) : Map.of();
                new TableStatsDialog("Table statistics: " + rt.second().getId(), stats).open();
            });

            return new VerticalLayout(useAsQuery, showStats);
        }).setHeader("Actions").setFlexGrow(1);
    }

    private Map<String, Integer> tableStats(Table<String> table)
    {
        Map<String, Integer> stats = new TreeMap<>();

        try
        {
            DataLakeService dl = new DataLakeService(this.dataLakeIp, this.user);
            Response response = dl.tableStats(table.getId());

            if (response.getResponseCode() != 200)
            {
                return new TreeMap<>();
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

    private void setQueryTable(Table<String> table)
    {
        List<List<String>> newTable = new ArrayList<>();
        int rows = table.rowCount();

        for (int row = 0; row < rows; row++)
        {
            Table.Row<String> tableRow = table.getRow(row);
            int columns = tableRow.size();
            List<String> columnItems = new ArrayList<>();

            for (int column = 0; column < columns; column++)
            {
                String cell = table.getRow(row).get(column);
                List<String> links = keywordSearch(cell);

                if (!links.isEmpty())
                {
                    columnItems.add(links.get(0));
                }

                else
                {
                    columnItems.add("");
                }
            }

            if (columnItems.stream().noneMatch(CharSequence::isEmpty))
            {
                newTable.add(columnItems);
            }
        }

        if (!newTable.isEmpty())
        {
            int columns = newTable.get(0).size();

            for (int i = 0; i < columns; i++)
            {
                int column = i;

                if (newTable.stream().allMatch(row -> row.get(column).isEmpty()))
                {
                    newTable.forEach(row -> row.remove(column));
                }
            }

            this.queryTable.setPresentationValue(newTable);
            renderEnteredValues();
        }

        else
        {
            Notification.show("Could not construct query table from this result table", 2500, Notification.Position.MIDDLE);
        }
    }
}
