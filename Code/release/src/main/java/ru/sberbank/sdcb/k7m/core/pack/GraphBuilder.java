package ru.sberbank.sdcb.k7m.core.pack;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.DefaultGraph;
import org.graphstream.stream.file.FileSinkImages;
import org.graphstream.ui.view.Viewer;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class GraphBuilder {

    public static void main(String[] args) throws IOException {

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        CTL ctl = mapper.readValue(new File(args[0]), CTL.class);

        Utils.Info info = Utils.build(ctl);



        DefaultGraph graph = new DefaultGraph("k7m");
        FileSinkImages pic = new FileSinkImages(FileSinkImages.OutputType.PNG, FileSinkImages.Resolutions.WXGA_8by5);
        pic.setLayoutPolicy(FileSinkImages.LayoutPolicy.COMPUTED_ONCE_AT_NEW_IMAGE);


        for (Workflow workflow : info.workflowByName.values()) {
            String name = workflow.getName();

            Node node = graph.addNode(name);
            node.addAttribute("ui.label", name);
            node.addAttribute("layout.weight", 2);
            if (name.contains("START")) {
                node.addAttribute("ui.style", "shape:circle;fill-color:yellow;size: 9px; text-alignment: under;");
            } else if (name.contains("FINISH")) {
                node.addAttribute("ui.style", "shape:circle;fill-color:green;size: 9px; text-alignment: under;");
            } else if (name.contains("BACKUP")) {
                node.addAttribute("ui.style", "shape:circle;fill-color:blue;size: 9px; text-alignment: under;");
            } else {
                node.addAttribute("ui.style", "shape:circle;size: 9px; text-alignment: under;");
            }
        }

        for (Map.Entry<String, Set<String>> entry : info.parentsMap.entrySet()) {
            for (String innerValue : entry.getValue()) {
                Edge edge = graph.addEdge(UUID.randomUUID().toString(), innerValue, entry.getKey(), true);
                edge.addAttribute("layout.weight", 2.5);
            }
        }

        //pic.writeAll(graph, "k7m.png");
        graph.addAttribute("layout.quality", 4);
        Viewer display = graph.display();

    }
}
