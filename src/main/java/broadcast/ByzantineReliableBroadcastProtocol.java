package broadcast;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import broadcast.messages.BrbEcho;
import broadcast.messages.BrbReady;
import broadcast.messages.BrbSend;
import pt.unl.fct.di.novasys.babel2.annotations.UponMessageIn;
import pt.unl.fct.di.novasys.babel2.channels.BabelChannel;
import pt.unl.fct.di.novasys.babel2.channels.BabelMessage;
import pt.unl.fct.di.novasys.babel2.channels.ComposedBabelChannel;
import pt.unl.fct.di.novasys.babel2.core.Babel;
import pt.unl.fct.di.novasys.babel2.core.BabelNode;
import pt.unl.fct.di.novasys.babel2.core.broadcast.BroadcastProviderBabelProtocol;
import pt.unl.fct.di.novasys.babel2.core.membership.MembershipConsumerBabelProtocol;

/**
 * Byzantine Reliable Broadcast (Bracha), com n >= 3f+1 réplicas.
 *
 * Diferença em relação à versão com emissor fixo: a instância é dada pelo
 * próprio payload (BrbPayloadPolicy.instanceId), e qualquer réplica pode
 * iniciá-la. Como cada réplica correta faz echo de um único valor por
 * instância, no máximo um valor é entregue por instância (consistência).
 */
public class ByzantineReliableBroadcastProtocol
        implements BroadcastProviderBabelProtocol, MembershipConsumerBabelProtocol {
    private static final Logger LOGGER = LogManager.getLogger(ByzantineReliableBroadcastProtocol.class);

    /** Estado de uma instância de broadcast. */
    private static final class Instance {
        String echoedValue;          // valor em que esta réplica fez echo (no máximo um)
        boolean sentReady;           // esta réplica já enviou READY?
        BabelNode origin;            // réplica cujo SEND aceitámos
        final Set<BabelNode> echoFrom = new HashSet<>();   // só conta o 1º ECHO de cada réplica
        final Set<BabelNode> readyFrom = new HashSet<>();  // só conta o 1º READY de cada réplica
        final Map<String, Integer> echoCount = new HashMap<>();
        final Map<String, Integer> readyCount = new HashMap<>();
        final Map<String, BabelMessage> values = new HashMap<>(); // valueId -> payload já validado
    }

    private final Set<BabelNode> members;
    private final Set<BabelNode> neighbors;
    private final BrbPayloadPolicy policy;

    private final int echoQuorum;    // > (n+f)/2  (= 2f+1 com n = 3f+1)
    private final int readyAmplify;  // f+1
    private final int deliverQuorum; // 2f+1

    private final Map<String, Instance> instances;
    private final Set<String> delivered;

    private Babel runtime;
    private BabelChannel channel;

    public ByzantineReliableBroadcastProtocol(List<BabelNode> membership, BrbPayloadPolicy policy) {
        this.members = Set.copyOf(membership);
        this.neighbors = new HashSet<>();
        this.policy = policy;
        this.instances = new HashMap<>();
        this.delivered = new HashSet<>();

        int n = membership.size();
        int f = (n - 1) / 3;
        this.echoQuorum = (n + f) / 2 + 1;
        this.readyAmplify = f + 1;
        this.deliverQuorum = 2 * f + 1;
        LOGGER.info("BRB with n={} f={} (echo quorum {}, ready {}, deliver {})",
                n, f, echoQuorum, readyAmplify, deliverQuorum);
    }

    @Override
    public void init(Babel runtime, ComposedBabelChannel channel) {
        this.runtime = runtime;
        this.channel = channel;
    }

    // ------------------------------------------------------------------ brbBroadcast

    @Override
    public void uponBroadcastRequest(BabelMessage message) {
        try {
            String id = policy.instanceId(message);
            if (delivered.contains(id)) {
                return;
            }
            Instance inst = instances.get(id);
            if (inst != null && inst.echoedValue != null) {
                return; // já recebemos o SEND de outra réplica: não disseminar outra vez
            }
            if (acceptValue(id, message) == null) {
                LOGGER.warn("Ignoring broadcast request with invalid payload {}", message);
                return;
            }
            sendToEveryone(new BrbSend(id, message));
            handleSend(id, message, runtime.getMyself());
        } catch (RuntimeException e) {
            LOGGER.warn("Could not broadcast {}: {}", message, e.toString());
        }
    }

    // ------------------------------------------------------------------ mensagens recebidas

    @UponMessageIn
    public void uponBrbSend(BrbSend msg, BabelNode sender) {
        if (!isReplica(sender)) {
            return;
        }
        try {
            handleSend(msg.instance(), msg.payload(), sender);
        } catch (RuntimeException e) {
            LOGGER.warn("Discarding malformed SEND from {}: {}", sender, e.toString());
        }
    }

    @UponMessageIn
    public void uponBrbEcho(BrbEcho msg, BabelNode sender) {
        if (!isReplica(sender)) {
            return;
        }
        try {
            handleEcho(msg.instance(), msg.payload(), sender);
        } catch (RuntimeException e) {
            LOGGER.warn("Discarding malformed ECHO from {}: {}", sender, e.toString());
        }
    }

    @UponMessageIn
    public void uponBrbReady(BrbReady msg, BabelNode sender) {
        if (!isReplica(sender)) {
            return;
        }
        try {
            handleReady(msg.instance(), msg.payload(), sender);
        } catch (RuntimeException e) {
            LOGGER.warn("Discarding malformed READY from {}: {}", sender, e.toString());
        }
    }

    // ------------------------------------------------------------------ algoritmo

    /** SEND: faz echo do primeiro valor válido que vemos nesta instância. */
    private void handleSend(String id, BabelMessage payload, BabelNode sender) {
        if (delivered.contains(id)) {
            return;
        }
        String valueId = acceptValue(id, payload);
        if (valueId == null) {
            return;
        }
        Instance inst = instances.get(id);
        if (inst.echoedValue != null) {
            return; // nunca fazer echo de dois valores na mesma instância
        }
        inst.echoedValue = valueId;
        inst.origin = sender;
        BabelMessage value = inst.values.get(valueId);
        sendToEveryone(new BrbEcho(id, value));
        handleEcho(id, value, runtime.getMyself());
    }

    /** ECHO: com echoQuorum echos do mesmo valor, envia READY. */
    private void handleEcho(String id, BabelMessage payload, BabelNode sender) {
        if (delivered.contains(id)) {
            return;
        }
        String valueId = acceptValue(id, payload);
        if (valueId == null) {
            return;
        }
        Instance inst = instances.get(id);
        if (!inst.echoFrom.add(sender)) {
            return; // esta réplica já fez echo nesta instância
        }
        int count = inst.echoCount.merge(valueId, 1, Integer::sum);
        if (count >= echoQuorum && !inst.sentReady) {
            sendReady(id, inst, valueId);
        }
    }

    /** READY: com f+1 amplifica, com 2f+1 entrega. */
    private void handleReady(String id, BabelMessage payload, BabelNode sender) {
        if (delivered.contains(id)) {
            return;
        }
        String valueId = acceptValue(id, payload);
        if (valueId == null) {
            return;
        }
        Instance inst = instances.get(id);
        if (!inst.readyFrom.add(sender)) {
            return; // esta réplica já enviou READY nesta instância
        }
        int count = inst.readyCount.merge(valueId, 1, Integer::sum);
        if (count >= readyAmplify && !inst.sentReady) {
            sendReady(id, inst, valueId); // pode entregar (via o nosso próprio READY)
        }
        if (count >= deliverQuorum && !delivered.contains(id)) {
            deliver(id, inst, valueId, sender);
        }
    }

    private void sendReady(String id, Instance inst, String valueId) {
        inst.sentReady = true;
        BabelMessage value = inst.values.get(valueId);
        sendToEveryone(new BrbReady(id, value));
        handleReady(id, value, runtime.getMyself());
    }

    private void deliver(String id, Instance inst, String valueId, BabelNode fallbackOrigin) {
        delivered.add(id);
        instances.remove(id); // liberta memória; mensagens futuras desta instância são ignoradas
        BabelMessage value = inst.values.get(valueId);
        BabelNode origin = inst.origin != null ? inst.origin : fallbackOrigin;
        LOGGER.debug("BRB deliver {} ({})", id, valueId);
        this.deliverMessage(runtime, value, origin);
    }

    /**
     * Valida o payload (uma vez por valor) e confirma que pertence a esta
     * instância (uma réplica bizantina não pode pôr um valor numa instância
     * que não é a dele). Devolve o valueId, ou null se o payload for inválido.
     */
    private String acceptValue(String id, BabelMessage payload) {
        if (id == null || payload == null || !id.equals(policy.instanceId(payload))) {
            return null;
        }
        String valueId = policy.valueId(payload);
        Instance inst = instances.get(id);
        if (inst != null && inst.values.containsKey(valueId)) {
            return valueId;
        }
        if (!policy.isValid(payload)) {
            return null;
        }
        instances.computeIfAbsent(id, k -> new Instance()).values.put(valueId, payload);
        return valueId;
    }

    // ------------------------------------------------------------------ utilitários

    private boolean isReplica(BabelNode sender) {
        if (!members.contains(sender)) {
            LOGGER.warn("Ignoring BRB message sent by {}, which is not a replica", sender);
            return false;
        }
        return true;
    }

    private void sendToEveryone(BabelMessage message) {
        for (BabelNode neighbor : neighbors) {
            channel.send(neighbor, message);
        }
    }

    @Override
    public void neighbourUp(BabelNode node) {
        neighbors.add(node);
    }

    @Override
    public void neighbourDown(BabelNode node) {
        neighbors.remove(node);
    }
}