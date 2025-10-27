package dk.aau.cs.dkwe.edao.jazero.web;

import com.vaadin.flow.component.page.AppShellConfigurator;
import com.vaadin.flow.component.page.LoadingIndicatorConfiguration;
import com.vaadin.flow.component.page.Push;
import com.vaadin.flow.server.AppShellSettings;
import com.vaadin.flow.server.ServiceInitEvent;
import com.vaadin.flow.server.VaadinServiceInitListener;
import com.vaadin.flow.theme.Theme;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * The entry point of the Spring Boot application.
 *
 * Use the @PWA annotation make the application installable on phones, tablets
 * and some desktop browsers.
 *
 */
@SpringBootApplication
@Push
@Theme("search-theme")
public class Main implements AppShellConfigurator, VaadinServiceInitListener
{
    @Bean
    public Clock clock()
    {
        return Clock.systemUTC();
    }

    @Override
    public void configurePage(AppShellSettings settings)
    {
        settings.addFavIcon("icon", "images/logo_old.png", "16x16");
    }

    @Override
    public void serviceInit(ServiceInitEvent serviceInitEvent)
    {
        serviceInitEvent.getSource().addUIInitListener(event -> {
            LoadingIndicatorConfiguration conf = event.getUI().getLoadingIndicatorConfiguration();
            conf.setApplyDefaultTheme(false);
        });
    }

    public static void main(String[] args)
    {
        SpringApplication.run(Main.class, args);
    }
}
