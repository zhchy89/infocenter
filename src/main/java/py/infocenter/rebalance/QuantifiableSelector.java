package py.infocenter.rebalance;

import edu.emory.mathcs.backport.java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import py.common.struct.Pair;
import py.common.struct.Quantifiable;

import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.function.ToIntFunction;

public class QuantifiableSelector<T> {
    private static final Logger logger = LoggerFactory.getLogger(QuantifiableSelector.class);

    private ToIntFunction<T> quantifier;

    public QuantifiableSelector() {
        quantifier = value -> {
            if (value instanceof Quantifiable) {
                return ((Quantifiable) value).value();
            } else {
                throw new IllegalArgumentException("no suitable Quantifier");
            }
        };
    }

    public QuantifiableSelector(ToIntFunction<T> quantifier) {
        this.quantifier = quantifier;
    }

    public Pair<Pair<T, Double>, Pair<T, Double>> selectTheMinAndMax(Collection<T> candidates, double threshold) {
        LinkedList<T> listCandidates = new LinkedList<>(candidates);
        Collections.sort(listCandidates, Comparator.comparingInt(quantifier));
        if (logger.isDebugEnabled()) {
            logger.debug("candidates {}", listCandidates);
        }
        int count = listCandidates.size();
        if (count < 2) {
            return new Pair<>();
        }
        double sum = 0;
        for (T t : listCandidates) {
            sum += quantifier.applyAsInt(t);
        }

        T candidateWithMaxValue = listCandidates.getLast();
        T candidateWithMinValue = listCandidates.getFirst();
        Pair<T, Double> maxCandidate = null;
        Pair<T, Double> minCandidate = null;

        double maxValue = quantifier.applyAsInt(candidateWithMaxValue);
        double minValue = quantifier.applyAsInt(candidateWithMinValue);

        if (count == 2) {
            if (maxValue / minValue > 1 + threshold) {
                maxCandidate = new Pair<>(candidateWithMaxValue, maxValue / minValue - 1);
            }
            if (minValue / maxValue < 1 - threshold) {
                minCandidate = new Pair<>(candidateWithMinValue, 1 - minValue / maxValue);
            }
        } else {
            double averageValueWithoutTheMinAndMax = (sum - minValue - maxValue) / (count - 2);

            if (maxValue / averageValueWithoutTheMinAndMax > 1 + threshold) {
                maxCandidate = new Pair<>(candidateWithMaxValue, maxValue / averageValueWithoutTheMinAndMax - 1);
            }

            if (minValue / averageValueWithoutTheMinAndMax < 1 - threshold) {
                minCandidate = new Pair<>(candidateWithMinValue, 1 - minValue / averageValueWithoutTheMinAndMax);
            }
        }

        return new Pair<>(minCandidate, maxCandidate);
    }

}
