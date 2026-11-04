use sqlx::Connection;

use super::super::reconciliation_d1_fixture as fixture;
use super::{Failure, apply_chain, d1_tuple_tests::fixture_plan};

#[tokio::test]
async fn terminal_admission_migration_backed_two_chain_orientations() {
    for (states, selected) in [
        (["terminal", "recovery_needed"], "a"),
        (["terminal", "recovery_needed"], "b"),
        (["recovery_needed", "terminal"], "a"),
        (["recovery_needed", "terminal"], "b"),
    ] {
        let (_root, mut connection) = fixture::database().await;
        fixture::seed(&mut connection, states).await;
        let mut plan = fixture_plan();
        fixture::populate(&mut connection, &mut plan, selected).await;
        let before = fixture::snapshot(&mut connection).await;
        let audit_before = fixture::audits(&mut connection).await;
        assert!(audit_before.is_empty());
        let mut transaction = connection.begin().await.unwrap();
        let result = apply_chain(&mut transaction, &plan).await;
        assert!(
            result.is_ok(),
            "states={states:?} selected={selected}: {result:?}"
        );
        transaction.commit().await.unwrap();
        let after = fixture::snapshot(&mut connection).await;
        for ((table, key, previous), (_, _, actual)) in before.iter().zip(&after) {
            let action = plan
                .actions
                .iter()
                .find(|action| &action.table_name == table && &action.canonical_pk == key)
                .unwrap();
            assert_eq!(action.before.as_ref(), Some(previous));
            assert_eq!(action.after.as_ref(), Some(actual), "{table}:{key}");
        }
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM historical_unknown_abandonments")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(count, 1);
        let audits = fixture::audits(&mut connection).await;
        assert_eq!(
            audits,
            [super::expected_chain_audit(&plan).unwrap().unwrap()]
        );
        let mut transaction = connection.begin().await.unwrap();
        assert!(matches!(
            apply_chain(&mut transaction, &plan).await,
            Err(Failure::ManualRequired)
        ));
        transaction.rollback().await.unwrap();
        assert_eq!(after, fixture::snapshot(&mut connection).await);
        assert_eq!(audits, fixture::audits(&mut connection).await);
    }
}
