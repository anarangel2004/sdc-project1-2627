package replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app.messages.Transfer;
import pt.unl.fct.di.novasys.babel2.annotations.UponBabelEvent;
import pt.unl.fct.di.novasys.babel2.annotations.UponBroadcastDeliver; // confirmar no exemplo do lab
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastConsumerBabelProtocol;
import replication.events.ExecuteTransaction;
import replication.events.SubmitTransaction;

/**
 * Variante 1: replicação sem consenso. Transferências submetidas são
 * disseminadas por BRB e executadas assim que o BRB as entrega (sem ordem
 * total: o Ledger trata da ordem por conta através do seq).
 */
public class BrbReplicationProtocol implements BroadcastConsumerBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(BrbReplicationProtocol.class);

    private Babel runtime;

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
    }

    @UponBabelEvent
    public void uponSubmitTransaction(SubmitTransaction submit) {
        broadcast(runtime, submit.transfer()); // brbBroadcast
    }

    @UponBroadcastDeliver // brbDeliver (confirmar a assinatura no exemplo do lab)
    public void uponDeliver(Transfer transfer, BabelNode origin) {
        try {
            LOGGER.debug("Delivered {} (from {})", transfer, origin);
            runtime.deliverEvent(new ExecuteTransaction(transfer));
        } catch (RuntimeException e) {
            LOGGER.warn("Could not execute delivered transfer: {}", e.toString());
        }
    }
}