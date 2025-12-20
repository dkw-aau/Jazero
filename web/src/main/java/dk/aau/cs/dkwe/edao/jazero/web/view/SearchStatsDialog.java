package dk.aau.cs.dkwe.edao.jazero.web.view;

import com.vaadin.flow.component.dialog.Dialog;
import com.vaadin.flow.component.html.Div;
import com.vaadin.flow.component.html.H4;

public class SearchStatsDialog extends Dialog
{
    public SearchStatsDialog(double reduction, double runtimeNanoseconds)
    {
        setHeaderTitle("Search statistics");
        setWidth("500px");
        setDraggable(true);
        setResizable(true);

        double seconds = runtimeNanoseconds / 1000000000;
        Div content = new Div();
        String runtimeValue = String.format("%.2f", seconds), reductionValue = String.format("%.2f", reduction);
        content.add(new H4("Search runtime: " + runtimeValue + "s"));
        content.add(new H4("Search space reduction: " + reductionValue + "%"));
        add(content);
    }
}
