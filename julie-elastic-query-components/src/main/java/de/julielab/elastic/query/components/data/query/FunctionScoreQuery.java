package de.julielab.elastic.query.components.data.query;

import java.util.List;

public class FunctionScoreQuery extends SearchServerQuery {
	public enum BoostMode {
		multiply, replace, sum, avg, max, min
	}

	public enum ScoreMode {
		multiply, replace, sum, avg, max, min
	}

	public SearchServerQuery query;
	public BoostMode boostMode = BoostMode.multiply;
	public ScoreMode scoreMode;
	/**
	 * NOTE: No implementation yet
	 */
	public Function scriptScore;
	/**
	 * NOTE: No implementation yet
	 */
	public Function weight;
	/**
	 * NOTE: No implementation yet
	 */
	public Function randomScore;
	public FieldValueFactor fieldValueFactor;

	public List<FunctionListItem> functions;
	/**
	 * NOTE: No implementation yet
	 */
	public DecayFunction linearDecay;
	/**
	 * NOTE: No implementation yet
	 */
	public DecayFunction expDecay;
	/**
	 * NOTE: No implementation yet
	 */
	public DecayFunction gaussDecay;

	public interface Function {

	};

	/**
	 * NOTE: No implementation yet
	 */
	public interface DecayFunction {
	};

	public static class FieldValueFactor implements Function {
		/**
		 * Eligible values for the {@link FieldValueFactor#modifier} field.
		 * 
		 * @see http 
		 *      ://www.elasticsearch.org/guide/en/elasticsearch/reference/current
		 *      /query-dsl-function-score-query.html, section 'Parameter
		 *      Description'
		 * 
		 */
		public enum Modifier {
			NONE, LOG, LOG1P, LOG2P, LN, LN1P, LN2P, SQUARE, SQRT, RECIPROCAL
		}

		/**
		 * A field holding a number to be used for scoring the respective
		 * document.
		 */
		public String field;
		/**
		 * Optional factor to multiply the field value with, defaults to 1.
		 */
		public float factor = 1f;
		/**
		 * Modifier to apply to the field value.
		 * 
		 * @see Modifier
		 */
		public Modifier modifier = Modifier.NONE;
		/**
		 * Value used if the document doesn’t have that field. The modifier and
		 * factor are still applied to it as though it were read from the
		 * document.
		 */
		public double missing = 1d;
	}

	/**
	 * To be used with {@link #function} to score according to a list of
	 * weighted functions that are applied according to an optional filter
	 * query.
	 * 
	 * @author faessler
	 * 
	 */
	public static class FunctionListItem {
		/**
		 * Optional. Restricts the documents to which this function is applied.
		 */
		public SearchServerQuery filter;
		public float weight = 1f;
		public Function function;
	}
}
