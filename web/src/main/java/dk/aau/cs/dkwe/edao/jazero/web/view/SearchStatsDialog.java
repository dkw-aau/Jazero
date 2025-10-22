package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H4;

public class SearchStatsDialog extends Dialog
{
    public SearchStatsDialog(double reduction, double runtime)
    {
        setHeaderTitle("Search statistics");
        setWidth("500px");
        setDraggable(true);
        setResizable(true);

        Div content = new Div();
        content.add(new H4("Search runtime: " + runtime + "ms"));
        content.add(new H4("Search space reduction: " + reduction + "%"));
        add(content);
    }
}
