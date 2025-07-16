package dev.vality.fistful.magista.endpoint;

import dev.vality.fistful.fistful_stat.FistfulStatisticsSrv;
import dev.vality.woody.thrift.impl.http.THServiceBuilder;
import jakarta.servlet.*;
import jakarta.servlet.annotation.WebServlet;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;

@WebServlet(urlPatterns = {"/stat", "/fistful/stat"})
public class FistfulStatisticsServlet extends GenericServlet {

    private Servlet thriftServlet;

    @Autowired
    private FistfulStatisticsSrv.Iface requestHandler;

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        thriftServlet = new THServiceBuilder()
                .build(FistfulStatisticsSrv.Iface.class, requestHandler);
    }

    @Override
    public void service(ServletRequest req, ServletResponse res) throws ServletException, IOException {
        thriftServlet.service(req, res);
    }
}
