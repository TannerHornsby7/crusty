use super::{OpIterator, TupleIterator};
use common::{AggOp, Attribute, CrustyError, DataType, Field, TableSchema, Tuple};
use std::cmp::{max, min};
use std::collections::HashMap;
use std::num;

/// Contains the index of the field to aggregate and the operator to apply to the column of each group. (You can add any other fields that you think are neccessary)
#[derive(Clone)]
pub struct AggregateField {
    /// Index of field being aggregated.
    pub field: usize,
    /// Agregate operation to aggregate the column with.
    pub op: AggOp,
}

// HELPER: merge
    // DESC: uses the enum for the aggregatefield operator to determine merge protocol and
    //       return the new field
    fn merge(aggregator: AggregateField, run: Option<Field>, new: Field, hash: Vec<Field>, group_tupes: &HashMap<Vec<Field>, Vec<Tuple>>, attr: usize) -> Field {
        // use a match on the aggregator's operator to determine the merge protocol
        let mut item = true;
        let mut running = Field::IntField(0);

        // use group tupes with hash and attr to find the total sum of the group
        // use group tupes with hash to find the total count of the group
        let mut sum = 0;
        let mut cnt = 0;
        for tuple in &group_tupes[&hash].clone() {
            let f = tuple.get_field(attr).unwrap();
            if let Field::IntField(n) = f {
                sum += n;
            }
            cnt += 1;
        }

        if run.is_some() {
            running = run.clone().unwrap();
        }

        match aggregator.op {
            AggOp::Count => {
                // if the operator is count, then increment the running field by 1
                running = Field::IntField(cnt);
                item = false;
            }
            AggOp::Sum => {
                // if the operator is sum, then add the new field to the running field
                running = Field::IntField(running.unwrap_int_field() + new.unwrap_int_field());
            }
            AggOp::Max => {
                // if the operator is max, then compare the running field to the new field
                // and set the running field to the max of the two
                running = max(running, new.clone());
            }
            AggOp::Min => {
                // if the operator is min, then compare the running field to the new field
                // and set the running field to the min of the two
                running = min(running, new.clone());
            }
            AggOp::Avg => {
                // if the operator is avg, then add the new field to the running field
                // and increment the running count by 1
                running = Field::IntField( sum / cnt);
            }
        }

        if run.is_some() {
            // return the new running field
            running
        }
        else {
            // if item is true, return the field of the new tuple, otherwise,
            // return 
            if item {
                return new
            }
            Field::IntField(1)
        }
    }

/// Computes an aggregation function over multiple columns and grouped by multiple fields. (You can add any other fields that you think are neccessary)
struct Aggregator {
    /// Aggregated fields.
    agg_fields: Vec<AggregateField>,
    /// Group by fields
    groupby_fields: Vec<usize>,
    /// Schema of the output.
    schema: TableSchema,
    /// Map of group by fields to the accumulated value of the aggregation (a single tuple).
    group_aggs: HashMap<Vec<Field>, Tuple>,
    /// store a vector of tuples for each field
    group_tupes: HashMap<Vec<Field>, Vec<Tuple>>,
}

impl Aggregator {
    /// Aggregator constructor.
    ///
    /// # Arguments
    ///
    /// * `agg_fields` - List of `AggregateField`s to aggregate over. `AggregateField`s contains the aggregation function and the field to aggregate over.
    /// * `groupby_fields` - Indices of the fields to groupby over.
    /// * `schema` - TableSchema of the form [groupby_field attributes ..., agg_field attributes ...]).
    fn new(
        agg_fields: Vec<AggregateField>,
        groupby_fields: Vec<usize>,
        schema: &TableSchema,
    ) -> Self {
        // initialize hashmaps to be empty
        let group_aggs = HashMap::new();
        let group_tupes = HashMap::new();
        Self { agg_fields, groupby_fields, schema: schema.clone(), group_aggs, group_tupes }
    }


    /// Handles the creation of groups for aggregation.
    ///
    /// If a group exists, then merge the tuple into the group's accumulated value.
    /// Otherwise, create a new group aggregate result.
    ///
    /// # Arguments
    ///
    /// * `tuple` - Tuple to add to a group.
    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        // use the groupby fields to create a key for the hashmap
        let mut groupby_fields = Vec::new();
        for i in &self.groupby_fields {
            groupby_fields.push(tuple.get_field(*i).unwrap().clone());
        }
        // update group tupes
        if self.group_tupes.contains_key(&groupby_fields) {
            let mut v = self.group_tupes[&groupby_fields].clone();
            v.push(tuple.clone());
            self.group_tupes.insert(groupby_fields.clone(), v);
        }
        else {
            self.group_tupes.insert(groupby_fields.clone(), vec![tuple.clone()]);
        }
        // modify the aggregate tuple
        // use the groupby_fields as a key, if its in the hm, then a group exits
        if self.group_aggs.contains_key(&groupby_fields) {
            // get a mutable reference to the matching aggregate tuple
            let mut agg_tup = self.group_aggs[&groupby_fields].clone();
            // if the group exists, then merge the current tuples using the aggregateField structs
            for (i, comp_field) in self.agg_fields.clone().into_iter().enumerate() {
                // update the group sums
                let num = tuple.get_field(comp_field.field).unwrap();
                // get the matching field from the tuple and the aggregate tuple
                let field = tuple.get_field(comp_field.field).unwrap();
                let agg_field = agg_tup.get_field(i).unwrap();
                // merge these fields based on the comp_field operator
                let res_field = merge(comp_field.clone(), Some(agg_field.clone()), field.clone(), groupby_fields.clone(), &self.group_tupes, comp_field.field);
                // update the aggregate tuple with the new field
                agg_tup.set_field(i, res_field);
            }
            // update the aggregate tuple in the hashmap
            self.group_aggs.insert(groupby_fields, agg_tup);
        } else {
            // create and insert the aggregate tuple from the schema and the input tuple
            // first create placeholder tuple from schema with arbitrary values
            let mut placeholder_vec: Vec<Field> = Vec::new();
            // we should use the aggregate_fields
            for agfield in self.agg_fields.clone() {
                let num = tuple.get_field(agfield.field).unwrap();
                //append attribute to the placeholder vec
                let x = tuple.get_field(agfield.field).unwrap().clone();
                placeholder_vec.push(x);
            }
            // get a tuple from the vector
            let mut agg_tup = Tuple::new(placeholder_vec);

            // now we merge
            for (i, comp_field) in self.agg_fields.clone().into_iter().enumerate() {
                // get the matching field from the tuple and the aggregate tuple
                let field = tuple.get_field(comp_field.field).unwrap();
                let res_field = merge(comp_field.clone(), None, field.clone(), groupby_fields.clone(), &self.group_tupes, comp_field.field);
                // merge these fields based on the comp_field operator
                // update the aggregate tuple with the new field
                agg_tup.set_field(i, res_field);
            }
            // update the aggregate tuple in the hashmap
            self.group_aggs.insert(groupby_fields, agg_tup);
        }
        // merge based on each aggregate field's operator
    }

    /// Returns a `TupleIterator` over the results.
    ///
    /// Resulting tuples must be of the form: (group by fields ..., aggregate fields ...)
    pub fn iterator(&self) -> TupleIterator {
        // use the hashmap to create a vector of tuples, then return a tuple iterator
        let mut tuples = Vec::new();
        for (key, value) in &self.group_aggs {
            let mut tuple = Vec::new();
            for field in key {
                tuple.push(field.clone());
            }
            for field in value.field_vals() {
                tuple.push(field.clone());
            }
            tuples.push(Tuple::new(tuple));
        }
        TupleIterator::new(tuples, self.schema.clone())
    }
}

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    /// Fields to groupby over.
    groupby_fields: Vec<usize>,
    /// Aggregation fields and corresponding aggregation functions.
    agg_fields: Vec<AggregateField>,
    /// Aggregation iterators for results.
    agg_iter: Option<TupleIterator>,
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Boolean if the iterator is open.
    open: bool,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
    /// Aggregator to aggregate the data.
    agg: Aggregator,
    prior_tuple: Option<Tuple>,
    tuples: Vec<Tuple>,
    tuple_idx: usize,
}

impl Aggregate {
    /// Aggregate constructor.
    ///
    /// # Arguments
    ///
    /// * `groupby_indices` - the indices of the group by fields
    /// * `groupby_names` - the names of the group_by fields in the final aggregation
    /// * `agg_indices` - the indices of the aggregate fields
    /// * `agg_names` - the names of the aggreagte fields in the final aggregation
    /// * `ops` - Aggregate operations, 1:1 correspondence with the indices in agg_indices
    /// * `child` - child operator to get the input data from.
    pub fn new(
        groupby_indices: Vec<usize>,
        groupby_names: Vec<&str>,
        agg_indices: Vec<usize>,
        agg_names: Vec<&str>,
        ops: Vec<AggOp>,
        child: Box<dyn OpIterator>,
    ) -> Self {
        // create a vector of aggregate fields
        let mut agg_fields = Vec::new();
        for i in 0..agg_indices.len() {
            agg_fields.push(AggregateField { field: agg_indices[i], op: ops[i] });
        }
        // create groupby fields
        let mut groupby_fields = Vec::new();
        for g in groupby_indices {
            groupby_fields.push(g);
        }
        // create a vector of attributes for creating the schema
        let mut attributes = Vec::new();
        for g in groupby_names {
            attributes.push(Attribute::new(g.to_string(), DataType::Int));
        }
        for agg in agg_names {
            attributes.push(Attribute::new(agg.to_string(), DataType::Int));
        }
        // create the schema
        let schema = TableSchema::new(attributes);
        // create aggregator
        let agg = Aggregator::new(agg_fields.clone(), groupby_fields.clone(), &schema);
        // create the agregate itterater
        let agg_iter = agg.iterator();
         // if there is no next child tuple, then return none
        
        
        let mut res = Self {
            groupby_fields,
            agg_fields,
            agg_iter: Some(agg_iter),
            schema,
            open: false,
            child,
            agg,
            prior_tuple: None,
            tuples: Vec::new(),
            tuple_idx: 0,
        };
        // open the child
        res.child.open().unwrap();
        // get all children tuples and aggregate them
        while let Some(child_tuple) = res.child.next().unwrap() {
            res.agg.merge_tuple_into_group(&child_tuple);
        }
        // get a new iterator
        res.agg_iter = Some(res.agg.iterator());
        // get a vector of tuples from the agg_iter
        let mut tuples = Vec::new();
        // open the iterator
        res.agg_iter.as_mut().unwrap().open().unwrap();
        while let Some(tuple) = res.agg_iter.as_mut().unwrap().next().unwrap() {
            tuples.push(tuple.clone());
        }
        // set the tuples field
        res.tuples = tuples;
        res
    }

}

impl OpIterator for Aggregate {
    fn open(&mut self) -> Result<(), CrustyError> {
        //check if its open
        if self.open {
            return Ok(())
        }
        // open the agg_iter
        // self.agg_iter.as_mut().unwrap().open()?;
        // open the child
        self.child.open()?;
        // set the open boolean to true
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Operator has not been opened")
        }
        // return the tuple at the tuple idx then increment the idx
        if self.tuple_idx < self.tuples.len() {
            let tuple = self.tuples[self.tuple_idx].clone();
            self.tuple_idx += 1;
            return Ok(Some(tuple));
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // check that its open
        if !self.open {
            panic!("Operator has not been opened")
        }
        // reset
        self.tuple_idx = 0;
        self.prior_tuple = None;
        // close the agg_iter
        self.agg_iter.as_mut().unwrap().close()?;
        // close the child
        self.child.close()?;
        // set the open boolean to false
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        // check that its open
        if !self.open {
            panic!("Operator has not been opened")
        }
        // rewind the child
        self.child.rewind()?;
        self.agg_iter.as_mut().unwrap().rewind()?;
        // set the tuple idx to 0
        self.tuple_idx = 0;
        // set the prior tuple to none
        self.prior_tuple = None;
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::opiterator::testutil::*;

    /// Creates a vector of tuples to create the following table:
    ///
    /// 1 1 3 E
    /// 2 1 3 G
    /// 3 1 4 A
    /// 4 2 4 G
    /// 5 2 5 G
    /// 6 2 5 G
    fn tuples() -> Vec<Tuple> {
        let tuples = vec![
            Tuple::new(vec![
                Field::IntField(1),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("E".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(2),
                Field::IntField(1),
                Field::IntField(3),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(3),
                Field::IntField(1),
                Field::IntField(4),
                Field::StringField("A".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(4),
                Field::IntField(2),
                Field::IntField(4),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(5),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
            Tuple::new(vec![
                Field::IntField(6),
                Field::IntField(2),
                Field::IntField(5),
                Field::StringField("G".to_string()),
            ]),
        ];
        tuples
    }

    mod aggregator {
        use super::*;
        use common::{DataType, Field};

        /// Set up testing aggregations without grouping.
        ///
        /// # Arguments
        ///
        /// * `op` - Aggregation Operation.
        /// * `field` - Field do aggregation operation over.
        /// * `expected` - The expected result.
        fn test_no_group(op: AggOp, field: usize, expected: i32) -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![Attribute::new("agg".to_string(), DataType::Int)]);
            let mut agg = Aggregator::new(vec![AggregateField { field, op }], Vec::new(), &schema);
            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(
                Field::IntField(expected),
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_merge_tuples_count() -> Result<(), CrustyError> {
            test_no_group(AggOp::Count, 0, 6)
        }

        #[test]
        fn test_merge_tuples_sum() -> Result<(), CrustyError> {
            test_no_group(AggOp::Sum, 1, 9)
        }

        #[test]
        fn test_merge_tuples_max() -> Result<(), CrustyError> {
            test_no_group(AggOp::Max, 0, 6)
        }

        #[test]
        fn test_merge_tuples_min() -> Result<(), CrustyError> {
            test_no_group(AggOp::Min, 0, 1)
        }

        #[test]
        fn test_merge_tuples_avg() -> Result<(), CrustyError> {
            test_no_group(AggOp::Avg, 0, 3)
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let _ = test_no_group(AggOp::Avg, 3, 3);
        }

        #[test]
        fn test_merge_multiple_ops() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("agg1".to_string(), DataType::Int),
                Attribute::new("agg2".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![
                    AggregateField {
                        field: 0,
                        op: AggOp::Max,
                    },
                    AggregateField {
                        field: 3,
                        op: AggOp::Count,
                    },
                ],
                Vec::new(),
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let expected = vec![Field::IntField(6), Field::IntField(6)];
            let mut ai = agg.iterator();
            ai.open()?;
            assert_eq!(Tuple::new(expected), ai.next()?.unwrap());
            Ok(())
        }

        #[test]
        fn test_merge_tuples_one_group() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);
            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(3, rows);
            Ok(())
        }

        /// Returns the count of the number of tuples in an OpIterator.
        ///
        /// This function consumes the iterator.
        ///
        /// # Arguments
        ///
        /// * `iter` - Iterator to count.
        pub fn num_tuples(iter: &mut impl OpIterator) -> Result<u32, CrustyError> {
            let mut counter = 0;
            while iter.next()?.is_some() {
                counter += 1;
            }
            Ok(counter)
        }

        #[test]
        fn test_merge_tuples_multiple_groups() -> Result<(), CrustyError> {
            let schema = TableSchema::new(vec![
                Attribute::new("group1".to_string(), DataType::Int),
                Attribute::new("group2".to_string(), DataType::Int),
                Attribute::new("agg".to_string(), DataType::Int),
            ]);

            let mut agg = Aggregator::new(
                vec![AggregateField {
                    field: 0,
                    op: AggOp::Sum,
                }],
                vec![1, 2],
                &schema,
            );

            let ti = tuples();
            for t in &ti {
                agg.merge_tuple_into_group(t);
            }

            let mut ai = agg.iterator();
            ai.open()?;
            let rows = num_tuples(&mut ai)?;
            assert_eq!(4, rows);
            Ok(())
        }
    }

    mod aggregate {
        use super::super::TupleIterator;
        use super::*;
        use common::{DataType, Field};

        fn tuple_iterator() -> TupleIterator {
            let names = vec!["1", "2", "3", "4"];
            let dtypes = vec![
                DataType::Int,
                DataType::Int,
                DataType::Int,
                DataType::String,
            ];
            let schema = TableSchema::from_vecs(names, dtypes);
            let tuples = tuples();
            TupleIterator::new(tuples, schema)
        }

        #[test]
        fn test_open() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            assert!(!ai.open);
            ai.open()?;
            assert!(ai.open);
            Ok(())
        }

        fn test_single_agg_no_group(
            op: AggOp,
            op_name: &str,
            col: usize,
            expected: Field,
        ) -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![col],
                vec![op_name],
                vec![op],
                Box::new(ti),
            );
            ai.open()?;
            assert_eq!(
                // Field::IntField(expected),
                expected,
                *ai.next()?.unwrap().get_field(0).unwrap()
            );
            assert_eq!(None, ai.next()?);
            Ok(())
        }

        #[test]
        fn test_single_agg() -> Result<(), CrustyError> {
            test_single_agg_no_group(AggOp::Count, "count", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Sum, "sum", 0, Field::IntField(21))?;
            test_single_agg_no_group(AggOp::Max, "max", 0, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Min, "min", 0, Field::IntField(1))?;
            test_single_agg_no_group(AggOp::Avg, "avg", 0, Field::IntField(3))?;
            test_single_agg_no_group(AggOp::Count, "count", 3, Field::IntField(6))?;
            test_single_agg_no_group(AggOp::Max, "max", 3, Field::StringField("G".to_string()))?;
            test_single_agg_no_group(AggOp::Min, "min", 3, Field::StringField("A".to_string()))
        }

        #[test]
        fn test_multiple_aggs() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![3, 0, 0],
                vec!["count", "avg", "max"],
                vec![AggOp::Count, AggOp::Avg, AggOp::Max],
                Box::new(ti),
            );
            ai.open()?;
            let first_row: Vec<Field> = ai.next()?.unwrap().field_vals().cloned().collect();
            assert_eq!(
                vec![Field::IntField(6), Field::IntField(3), Field::IntField(6)],
                first_row
            );
            ai.close()
        }

        /// Consumes an OpIterator and returns a corresponding 2D Vec of fields
        pub fn iter_to_vec(iter: &mut impl OpIterator) -> Result<Vec<Vec<Field>>, CrustyError> {
            let mut rows = Vec::new();
            iter.open()?;
            while let Some(t) = iter.next()? {
                rows.push(t.field_vals().cloned().collect());
            }
            iter.close()?;
            Ok(rows)
        }

        #[test]
        fn test_multiple_aggs_groups() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![1, 2],
                vec!["group1", "group2"],
                vec![3, 0],
                vec!["count", "max"],
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            let mut result = iter_to_vec(&mut ai)?;
            result.sort();
            let expected = vec![
                vec![
                    Field::IntField(1),
                    Field::IntField(3),
                    Field::IntField(2),
                    Field::IntField(2),
                ],
                vec![
                    Field::IntField(1),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(3),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(4),
                    Field::IntField(1),
                    Field::IntField(4),
                ],
                vec![
                    Field::IntField(2),
                    Field::IntField(5),
                    Field::IntField(2),
                    Field::IntField(6),
                ],
            ];
            assert_eq!(expected, result);
            ai.open()?;
            let num_rows = num_tuples(&mut ai)?;
            ai.close()?;
            assert_eq!(4, num_rows);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.next().unwrap();
        }

        #[test]
        fn test_close() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            assert!(ai.open);
            ai.close()?;
            assert!(!ai.open);
            Ok(())
        }

        #[test]
        #[should_panic]
        fn test_close_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.close().unwrap();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                Vec::new(),
                Vec::new(),
                vec![0],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.rewind().unwrap();
        }

        #[test]
        fn test_rewind() -> Result<(), CrustyError> {
            let ti = tuple_iterator();
            let mut ai = Aggregate::new(
                vec![2],
                vec!["group"],
                vec![3],
                vec!["count"],
                vec![AggOp::Count],
                Box::new(ti),
            );
            ai.open()?;
            let count_before = num_tuples(&mut ai);
            ai.rewind()?;
            let count_after = num_tuples(&mut ai);
            ai.close()?;
            assert_eq!(count_before, count_after);
            Ok(())
        }

        #[test]
        fn test_get_schema() {
            let mut agg_names = vec!["count", "max"];
            let mut groupby_names = vec!["group1", "group2"];
            let ti = tuple_iterator();
            let ai = Aggregate::new(
                vec![1, 2],
                groupby_names.clone(),
                vec![3, 0],
                agg_names.clone(),
                vec![AggOp::Count, AggOp::Max],
                Box::new(ti),
            );
            groupby_names.append(&mut agg_names);
            let expected_names = groupby_names;
            let schema = ai.get_schema();
            for (i, attr) in schema.attributes().enumerate() {
                assert_eq!(expected_names[i], attr.name());
                assert_eq!(DataType::Int, *attr.dtype());
            }
        }
    }
}
