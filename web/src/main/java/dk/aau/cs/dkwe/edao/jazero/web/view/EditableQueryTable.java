package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.componentfactory.Autocomplete;
import com.vaadin.flow.component.button.Button;
import com.vaadin.flow.component.customfield.CustomField;
import com.vaadin.flow.component.formlayout.FormLayout;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.icon.VaadinIcon;
import com.vaadin.flow.dom.Style;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class EditableQueryTable extends CustomField<List<List<String>>>
{
    private final Div grid = new Div();
    private final List<List<Autocomplete>> cells = new ArrayList<>();
    private int rows = 3;
    private int cols = 3;
    private BiConsumer<Autocomplete.AucompleteChangeEvent, Autocomplete> fieldChangeListener;
    private BiConsumer<Autocomplete.AutocompleteValueAppliedEvent, Autocomplete> valueAppliedListener;

    private final Button addRow = new Button("Add row", VaadinIcon.PLUS.create());
    private final Button removeRow = new Button("Remove row", VaadinIcon.MINUS.create());
    private final Button addCol = new Button("Add column", VaadinIcon.PLUS.create());
    private final Button removeCol = new Button("Remove column", VaadinIcon.MINUS.create());
    private final Button clearAll = new Button("Clear all", VaadinIcon.ERASER.create());

    public EditableQueryTable(BiConsumer<Autocomplete.AucompleteChangeEvent, Autocomplete> fieldChangeListener,
                              BiConsumer<Autocomplete.AutocompleteValueAppliedEvent, Autocomplete> valueAppliedListener)
    {
        this.fieldChangeListener = fieldChangeListener;
        this.valueAppliedListener = valueAppliedListener;
        initCells(rows, cols);
        buildGrid();

        this.addRow.addClickListener(e -> {
            List<Autocomplete> newRow = new ArrayList<>();
            this.rows++;

            for (int c = 0; c < this.cols; c++)
            {
                newRow.add(newCell());
            }

            this.cells.add(newRow);
            renderGrid();
        });

        this.removeRow.addClickListener(e -> {
            if (this.rows > 1)
            {
                this.rows--;
                this.cells.remove(cells.size() - 1);
                renderGrid();
            }
        });

        this.addCol.addClickListener(e -> {
            this.cols++;

            for (var row : this.cells)
            {
                row.add(newCell());
            }

            renderGrid();
        });

        this.removeCol.addClickListener(e -> {
            if (this.cols > 1)
            {
                this.cols--;

                for (var row : this.cells)
                {
                    row.remove(row.size() - 1);
                }

                renderGrid();
            }
        });

        this.clearAll.addClickListener(e -> {
            for (var row : this.cells)
            {
                for (var tf : row)
                {
                    tf.setValue("");
                }
            }

            renderGrid();
            updateValue();
        });

        FormLayout controls = new FormLayout(this.addRow, this.removeRow, this.addCol, this.removeCol, this.clearAll);
        Div wrapper = new Div(this.grid, controls);
        add(wrapper);
        addValueChangeListener(e -> {});
    }

    private Autocomplete newCell()
    {
        Autocomplete textField = new Autocomplete();
        textField.getStyle().set("font-family", "Courier New");
        textField.addChangeListener(event -> this.fieldChangeListener.accept(event, textField));
        textField.addAutocompleteValueAppliedListener(event -> this.valueAppliedListener.accept(event, textField));

        return textField;
    }

    private void initCells(int rows, int columns)
    {
        this.cells.clear();

        for (int i = 0; i < rows; i++)
        {
            List<Autocomplete> tableRow = new ArrayList<>();
            List<String> tableRowValue = new ArrayList<>();

            for (int j = 0; j < columns; j++)
            {
                tableRow.add(newCell());
                tableRowValue.add("");
            }

            this.cells.add(tableRow);
        }
    }

    private void buildGrid()
    {
        this.grid.getStyle().set("display", "grid");
        this.grid.getStyle().set("gap", "8px");
        renderGrid();
    }

    private void renderGrid()
    {
        Style style = this.grid.getStyle();
        style.set("grid-template-columns", "repeat(" + this.cols + ", 180px)");
        this.grid.removeAll();

        for (var row : this.cells)
        {
            for (Autocomplete textField : row)
            {
                this.grid.add(textField);
            }
        }
    }

    @Override
    protected List<List<String>> generateModelValue()
    {
        List<List<String>> table = new ArrayList<>(this.rows);

        for (int row = 0; row < this.rows; row++)
        {
            List<String> tableRow = new ArrayList<>(this.cols);

            for (int column = 0; column < this.cols; column++)
            {
                tableRow.add(this.cells.get(row).get(column).getValue());
            }

            table.add(tableRow);
        }

        return table;
    }

    @Override
    protected void setPresentationValue(List<List<String>> newValue)
    {
        if (newValue == null || newValue.isEmpty())
        {
            return;
        }

        this.rows = newValue.size();
        this.cols = newValue.get(0).size();
        initCells(this.rows, this.cols);

        for (int i = 0; i < this.rows; i++)
        {
            for (int j = 0; j < this.cols; j++)
            {
                this.cells.get(i).get(j).setValue(newValue.get(i).get(j));
            }
        }

        renderGrid();
        updateValue();
    }
}
