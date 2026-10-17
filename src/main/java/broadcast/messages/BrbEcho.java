package broadcast.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

@BabelIPMessage(id = 202)
public record BrbEcho(String instance, BabelMessage payload) implements BabelMessage {
}