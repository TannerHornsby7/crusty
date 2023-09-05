use super::{OpIterator, TupleIterator};
use common::{CrustyError, Field, SimplePredicateOp, TableSchema, Tuple};
use std::collections::HashMap;

/// Compares the fields of two tuples using a predicate. (You can add any other fields that you think are neccessary)
pub struct JoinPredicate {
    /// Operation to comapre the fields with.
    op: SimplePredicateOp,
    /// Index of the field of the left table (tuple).
    left_index: usize,
    /// Index of the field of the right table (tuple).
    right_index: usize,
}

impl JoinPredicate {
    /// Constructor that determines if two tuples satisfy the join condition.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation to compare the two fields with.
    /// * `left_index` - Index of the field to compare in the left tuple.
    /// * `right_index` - Index of the field to compare in the right tuple.
    fn new(op: SimplePredicateOp, left_index: usize, right_index: usize) -> Self {
        JoinPredicate {
            op,
            left_index,
            right_index,
        }
    }
}

/// Nested loop join implementation. (You can add any other fields that you think are neccessary)
pub struct Join {
    /// Join condition.
    predicate: JoinPredicate,
    /// Left child node.
    left_child: Box<dyn OpIterator>,
    /// Right child node.
    right_child: Box<dyn OpIterator>,
    /// Schema of the result.
    schema: TableSchema,
    /// Boolean determining if iterator is open.
    open: bool,
    /// Keep track of the current outer tuple.
    out_tup: Option<Tuple>,
}

impl Join {
    /// Join constructor. Creates a new node for a nested-loop join.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        // iterate through the fields of the left and right child to create the schema of the result
        let mut attributes = Vec::new();
        let left_schema = left_child.get_schema();
        let right_schema = right_child.get_schema();
        // add the fields of the left child using the .attributes iterator
        for attr in left_schema.attributes() {
            attributes.push(attr.clone());
        }
        // add the fields of the right child using the .attributes iterator
        for attr in right_schema.attributes() {
            attributes.push(attr.clone());
        }
        let schema = TableSchema::new(attributes);
        // create the predicate
        let predicate = JoinPredicate::new(op, left_index, right_index);
        Join {
            predicate,
            left_child,
            right_child,
            schema,
            open: false,
            out_tup: None,
        }
    }
}

impl OpIterator for Join {
    fn open(&mut self) -> Result<(), CrustyError> {
        // open the child nodes first
        self.left_child.open()?;
        self.right_child.open()?;
        // set open to true
        self.open = true;
        Ok(())
    }

    /// Calculates the next tuple for a nested loop join.
    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        // Check if the join operator is open
        if !self.open {
            panic!("Operator has not been opened");
        }
        // if the outer tuple is None, get next and reset out_tup
        if self.out_tup.is_none() {
            self.out_tup = self.left_child.next()?;
        }
        // iterate the right tuple, if it is None, reset the outer tuple and iterate again
        if let Some(ltuple) = &self.out_tup {
            let rnext = self.right_child.next()?;
            if let Some(rtuple) = rnext {
                // check if the join condition is satisfied
                if self.predicate.op.compare(
                    ltuple.get_field(self.predicate.left_index).unwrap(),
                    rtuple.get_field(self.predicate.right_index).unwrap(),
                ) {
                    // create a new tuple with the fields of the left and right child
                    let mut new_field_vals = Vec::new();
                    for i in 0..ltuple.size() {
                        new_field_vals.push(ltuple.get_field(i).unwrap().clone());
                    }
                    for i in 0..rtuple.size() {
                        new_field_vals.push(rtuple.get_field(i).unwrap().clone());
                    }
                    return Ok(Some(Tuple::new(new_field_vals)));
                } else {
                    // if the join condition is not satisfied, iterate the right child again
                    return self.next();
                }
            }
            // if right is none, we are at the end of the right child, reset right and increment left, updating out_tup
            else {
                self.right_child.rewind()?;
                self.out_tup = self.left_child.next()?;
                if self.out_tup.is_none() {
                    return Ok(None);
                }
                return self.next();
            }
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // check if open was false
        if !self.open {
            panic!("Operator has not been opened");
        }
        // set open to false
        self.open = false;
        // close the child nodes
        self.left_child.close()?;
        self.right_child.close()?;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        // rewind the child nodes
        self.left_child.rewind()?;
        self.right_child.rewind()?;
        Ok(())
    }

    /// return schema of the result
    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

/// Hash equi-join implementation. (You can add any other fields that you think are neccessary)
pub struct HashEqJoin {
    predicate: JoinPredicate,

    left_child: Box<dyn OpIterator>,
    right_child: Box<dyn OpIterator>,

    schema: TableSchema,
    // inner relation hash table
    hash_table: HashMap<Field, Vec<Tuple>>,
    open: bool,
}

impl HashEqJoin {
    /// Constructor for a hash equi-join operator.
    ///
    /// # Arguments
    ///
    /// * `op` - Operation in join condition.
    /// * `left_index` - Index of the left field in join condition.
    /// * `right_index` - Index of the right field in join condition.
    /// * `left_child` - Left child of join operator.
    /// * `right_child` - Left child of join operator.
    #[allow(dead_code)]
    pub fn new(
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
        left_child: Box<dyn OpIterator>,
        right_child: Box<dyn OpIterator>,
    ) -> Self {
        // we first create the schema by iterating through the fields of the
        // left and right children
        // iterate through the fields of the left and right child to create the schema of the result
        let mut attributes = Vec::new();
        let left_schema = left_child.get_schema();
        let right_schema = right_child.get_schema();
        // add the fields of the left child using the .attributes iterator
        for attr in left_schema.attributes() {
            attributes.push(attr.clone());
        }
        // add the fields of the right child using the .attributes iterator
        for attr in right_schema.attributes() {
            attributes.push(attr.clone());
        }
        // now we create our joined schema
        let schema = TableSchema::new(attributes);
        // now we make the predicate
        let predicate = JoinPredicate::new(op, left_index, right_index);
        // build a hashtable for one of the children, we will arbitrarily choose right
        let hash_table: HashMap<Field, Vec<Tuple>> = HashMap::new();
        // now we create the base struct with this empty hash map
        let mut res = HashEqJoin {
            predicate,
            left_child,
            right_child,
            schema,
            hash_table,
            open: false,
        };
        // populaet the hash table
        // open the right child
        res.right_child.open().unwrap();

        // iterate through the right child
        while let Some(tuple) = res.right_child.next().unwrap() {
            // get the field we are joining on
            let field = tuple.get_field(right_index).unwrap();
            // get the hash of the field
            let hash = field;
            if res.hash_table.contains_key(hash) {
                // if the hash is already in the hash table, we append the tuple to the vector
                res.hash_table.get_mut(hash).unwrap().push(tuple.clone());
            } else {
                // if the hash is not in the hash table, we create a new vector and insert the tuple
                let vec = vec![tuple.clone()];
                res.hash_table.insert(hash.clone(), vec);
            }
        }
        // reset and close the right child
        res.right_child.rewind().unwrap();
        res.right_child.close().unwrap();
        res
    }
}

impl OpIterator for HashEqJoin {
    fn open(&mut self) -> Result<(), CrustyError> {
        // exact same as nested loop join
        self.left_child.open()?;
        self.right_child.open()?;
        self.open = true;
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        // easier than the nested loop join, as we can just use a hashmap
        // to store the tuples based on the attribute we are joining on
        // and iterate through the tuples in the hashmap

        // first we must check that the operator is open
        if !self.open {
            panic!("Operator has not been opened");
        }
        // now we iterate through the left and compare each element with the
        // hash table, if it is in the hashtable, we join the tuples
        if let Some(ltuple) = self.left_child.next().unwrap()  {
            // compare it with the HashTable
            let field = ltuple.get_field(self.predicate.left_index).unwrap();
            let hash = field;
            if self.hash_table.contains_key(hash) {
                // if the hash is in the hash table, we append the fields in the left tuple to the vector
                // and then we iterate through the tuples in the hash table
                if let Some(tuple) = self.hash_table.get(hash).unwrap().iter().next() {
                    // create a new tuple with the fields of the left and right child
                    let mut new_field_vals = Vec::new();
                    for i in 0..ltuple.size() {
                        new_field_vals.push(ltuple.get_field(i).unwrap().clone());
                    }
                    for i in 0..tuple.size() {
                        new_field_vals.push(tuple.get_field(i).unwrap().clone());
                    }
                    return Ok(Some(Tuple::new(new_field_vals)));
                }
            }
            else {
                // otherwise, the hash is not in the hash table, so we iterate the left child again
                return self.next();
            }            
        }
        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        // close the children, then set open to false
        self.left_child.close()?;
        self.right_child.close()?;
        self.open = false;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        // rewind the children
        self.left_child.rewind()?;
        self.right_child.rewind()?;
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
    use common::testutil::*;

    const WIDTH1: usize = 2;
    const WIDTH2: usize = 3;
    enum JoinType {
        NestedLoop,
        HashEq,
    }

    pub fn scan1() -> TupleIterator {
        let tuples = create_tuple_list(vec![vec![1, 2], vec![3, 4], vec![5, 6], vec![7, 8]]);
        let ts = get_int_table_schema(WIDTH1);
        TupleIterator::new(tuples, ts)
    }

    pub fn scan2() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 3],
            vec![2, 3, 4],
            vec![3, 4, 5],
            vec![4, 5, 6],
            vec![5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3],
            vec![3, 4, 3, 4, 5],
            vec![5, 6, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn gt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![3, 4, 1, 2, 3], // 1, 2 < 3
            vec![3, 4, 2, 3, 4],
            vec![5, 6, 1, 2, 3], // 1, 2, 3, 4 < 5
            vec![5, 6, 2, 3, 4],
            vec![5, 6, 3, 4, 5],
            vec![5, 6, 4, 5, 6],
            vec![7, 8, 1, 2, 3], // 1, 2, 3, 4, 5 < 7
            vec![7, 8, 2, 3, 4],
            vec![7, 8, 3, 4, 5],
            vec![7, 8, 4, 5, 6],
            vec![7, 8, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 2, 3, 4], // 1 < 2, 3, 4, 5
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 4, 5, 6], // 3 < 4, 5
            vec![3, 4, 5, 6, 7],
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    pub fn lt_or_eq_join() -> TupleIterator {
        let tuples = create_tuple_list(vec![
            vec![1, 2, 1, 2, 3], // 1 <= 1, 2, 3, 4, 5
            vec![1, 2, 2, 3, 4],
            vec![1, 2, 3, 4, 5],
            vec![1, 2, 4, 5, 6],
            vec![1, 2, 5, 6, 7],
            vec![3, 4, 3, 4, 5], // 3 <= 3, 4, 5
            vec![3, 4, 4, 5, 6],
            vec![3, 4, 5, 6, 7],
            vec![5, 6, 5, 6, 7], // 5 <= 5
        ]);
        let ts = get_int_table_schema(WIDTH1 + WIDTH2);
        TupleIterator::new(tuples, ts)
    }

    fn construct_join(
        ty: JoinType,
        op: SimplePredicateOp,
        left_index: usize,
        right_index: usize,
    ) -> Box<dyn OpIterator> {
        let s1 = Box::new(scan1());
        let s2 = Box::new(scan2());
        match ty {
            JoinType::NestedLoop => Box::new(Join::new(op, left_index, right_index, s1, s2)),
            JoinType::HashEq => Box::new(HashEqJoin::new(op, left_index, right_index, s1, s2)),
        }
    }

    fn test_get_schema(join_type: JoinType) {
        let op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let expected = get_int_table_schema(WIDTH1 + WIDTH2);
        let actual = op.get_schema();
        assert_eq!(&expected, actual);
    }

    fn test_next_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.next().unwrap();
    }

    fn test_close_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.close().unwrap();
    }

    fn test_rewind_not_open(join_type: JoinType) {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.rewind().unwrap();
    }

    fn test_rewind(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        op.open()?;
        while op.next()?.is_some() {}
        op.rewind()?;

        let mut eq_join = eq_join();
        eq_join.open()?;

        let acutal = op.next()?;
        let expected = eq_join.next()?;
        assert_eq!(acutal, expected);
        Ok(())
    }

    fn test_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::Equals, 0, 0);
        let mut eq_join = eq_join();
        op.open()?;
        eq_join.open()?;
        match_all_tuples(op, Box::new(eq_join))
    }

    fn test_gt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::GreaterThan, 0, 0);
        let mut gt_join = gt_join();
        op.open()?;
        gt_join.open()?;
        match_all_tuples(op, Box::new(gt_join))
    }

    fn test_lt_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThan, 0, 0);
        let mut lt_join = lt_join();
        op.open()?;
        lt_join.open()?;
        match_all_tuples(op, Box::new(lt_join))
    }

    fn test_lt_or_eq_join(join_type: JoinType) -> Result<(), CrustyError> {
        let mut op = construct_join(join_type, SimplePredicateOp::LessThanOrEq, 0, 0);
        let mut lt_or_eq_join = lt_or_eq_join();
        op.open()?;
        lt_or_eq_join.open()?;
        match_all_tuples(op, Box::new(lt_or_eq_join))
    }

    mod join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn close_not_open() {
            test_close_not_open(JoinType::NestedLoop);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::NestedLoop);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::NestedLoop)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::NestedLoop)
        }

        #[test]
        fn gt_join() -> Result<(), CrustyError> {
            test_gt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_join() -> Result<(), CrustyError> {
            test_lt_join(JoinType::NestedLoop)
        }

        #[test]
        fn lt_or_eq_join() -> Result<(), CrustyError> {
            test_lt_or_eq_join(JoinType::NestedLoop)
        }
    }

    mod hash_join {
        use super::*;

        #[test]
        fn get_schema() {
            test_get_schema(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn next_not_open() {
            test_next_not_open(JoinType::HashEq);
        }

        #[test]
        #[should_panic]
        fn rewind_not_open() {
            test_rewind_not_open(JoinType::HashEq);
        }

        #[test]
        fn rewind() -> Result<(), CrustyError> {
            test_rewind(JoinType::HashEq)
        }

        #[test]
        fn eq_join() -> Result<(), CrustyError> {
            test_eq_join(JoinType::HashEq)
        }
    }
}
