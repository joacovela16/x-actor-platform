package xplat.flow.example;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import xplat.flow.model.NodeDescriptor;
import xplat.flow.model.NodeInput;
import xplat.flow.model.NodeOutput;
import xplat.flow.plugin.JavaFlowPlugin;

/** A node written in Java: writes `text` in upper case to `shouted`. */
public final class ShoutNode implements JavaFlowPlugin.Node {

  @Override
  public NodeDescriptor descriptor() {
    return JavaFlowPlugin.descriptor(
        "shout",
        "Example",
        "Shout",
        JavaFlowPlugin.Pure(),
        List.of(JavaFlowPlugin.field("text", JavaFlowPlugin.Str(), true, "The text to shout")),
        Map.of("shouted", JavaFlowPlugin.Str()));
  }

  @Override
  public CompletableFuture<NodeOutput> run(NodeInput input) {
    return CompletableFuture.completedFuture(
        JavaFlowPlugin.text(input, "text")
            .map(t -> JavaFlowPlugin.ok(Map.of("shouted", JavaFlowPlugin.string(t.toUpperCase()))))
            .orElseGet(() -> JavaFlowPlugin.failed("text is empty")));
  }
}
