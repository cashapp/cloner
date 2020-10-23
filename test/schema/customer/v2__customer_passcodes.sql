CREATE TABLE `customer_passcodes` (
  `id` bigint(20) NOT NULL,
  `customer_id` bigint(20) NOT NULL,
  `original_customer_id` bigint(20) NOT NULL,
  `token` varchar(255) NOT NULL,
  `fidelius_token` varchar(255) NOT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `unlinked_at` timestamp NULL DEFAULT NULL,
  `version` bigint(20) NOT NULL DEFAULT '0',
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `updated_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `last_verified_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unq_customer_id_active` (`customer_id`,`active`),
  KEY `idx_fidelius_token` (`fidelius_token`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC