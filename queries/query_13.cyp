MATCH path = (n_0:aws_iam_user {org_id: 2, to_delete: false, entity_info_status: 1}) -[r_0:CAN_ASSUME*1..3] ->(n_1 {org_id: 2,to_delete: false, entity_info_status: 1}) where n_1 = n_0 RETURN path
