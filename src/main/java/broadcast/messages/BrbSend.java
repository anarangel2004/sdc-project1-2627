package broadcast.messages;

import pt.unl.fct.di.novasys.babel2.channels.BabelIPMessage;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

@BabelIPMessage(id = 201)
public record BrbSend(String instance, BabelMessage payload) implements BabelMessage {
}