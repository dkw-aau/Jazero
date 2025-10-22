package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H4;

import java.util.Map;

public class DataLakeStatsDialog extends Dialog
{
    public DataLakeStatsDialog(String title, Map<String, Integer> stats)
    {
        Div content = new Div();
        setHeaderTitle(title);
        setWidth("500px");
        setDraggable(true);
        setResizable(true);

        for (var entry : stats.entrySet())
        {
            content.add(new H4(entry.getKey() + ": " + entry.getValue()));
        }

        setDraggable(true);
        setResizable(true);
        add(content);
    }
}
