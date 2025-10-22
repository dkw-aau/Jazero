package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.grid.Grid;

import java.util.List;

public class TablePreviewDialog extends Dialog
{
    public TablePreviewDialog(String title, List<List<String>> table)
    {
        setHeaderTitle(title);
        setWidth("800px");
        setHeight("600px");
        setDraggable(true);
        setResizable(true);

        Grid<List<String>> grid = new Grid<>();
        int maxCols = table.stream().mapToInt(List::size).max().orElse(0);

        for (int c = 0; c < maxCols; c++)
        {
            final int idx = c;
            grid.addColumn(row -> idx < row.size() ? row.get(idx) : "").setHeader("Col " + (c+1));
        }

        grid.setItems(table);
        add(grid);
    }
}
