package replication;

import app.ledger.Ledger;
import app.messages.Transfer;
import broadcast.BrbPayloadPolicy;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;

/**
 * Instância = conta#seq. Duas transferências conflituosas (mesma conta e
 * mesmo seq) caem na mesma instância de BRB, logo no máximo uma é entregue
 * por réplicas corretas: impede o double spending sem ordem total.
 */
public class TransferBrbPolicy implements BrbPayloadPolicy {

    // Ledger usado só para validar (não partilhado com o PaymentService)
    private final Ledger validator;

    public TransferBrbPolicy(Ledger validator) {
        this.validator = validator;
    }

    @Override
    public String instanceId(BabelMessage payload) {
        Transfer t = asTransfer(payload);
        return "tx:" + t.from() + "#" + t.seq();
    }

    @Override
    public String valueId(BabelMessage payload) {
        return asTransfer(payload).id();
    }

    @Override
    public boolean isValid(BabelMessage payload) {
        return payload instanceof Transfer t && validator.isValid(t);
    }

    private static Transfer asTransfer(BabelMessage payload) {
        if (payload instanceof Transfer t) {
            return t;
        }
        throw new IllegalArgumentException("Not a transfer: " + payload);
    }
}