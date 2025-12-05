-- phpMyAdmin SQL Dump
-- version 5.2.1
-- https://www.phpmyadmin.net/
--
-- Host: 127.0.0.1:3306
-- Generation Time: Oct 01, 2025 at 10:13 PM
-- Server version: 9.1.0
-- PHP Version: 8.3.14

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
START TRANSACTION;
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;

--
-- Database: `knowauthority`
--

-- --------------------------------------------------------

--
-- Table structure for table `oai_identify`
--

DROP TABLE IF EXISTS `oai_identify`;
CREATE TABLE IF NOT EXISTS `oai_identify` (
  `id` int NOT NULL AUTO_INCREMENT,
  `repository_name` varchar(255) DEFAULT NULL,
  `base_url` text,
  `protocol_version` varchar(50) DEFAULT NULL,
  `admin_email` varchar(255) DEFAULT NULL,
  `earliest_datestamp` varchar(50) DEFAULT NULL,
  `deleted_record` varchar(50) DEFAULT NULL,
  `granularity` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `oai_records`
--

DROP TABLE IF EXISTS `oai_records`;
CREATE TABLE IF NOT EXISTS `oai_records` (
  `id` int NOT NULL AUTO_INCREMENT,
  `repository` int NOT NULL DEFAULT '0',
  `oai_identifier` varchar(255) DEFAULT NULL,
  `datestamp` varchar(50) DEFAULT NULL,
  `setSpec` varchar(255) DEFAULT NULL,
  `deleted` tinyint(1) DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `oai_identifier` (`oai_identifier`(50),`repository`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `oai_sets`
--

DROP TABLE IF EXISTS `oai_sets`;
CREATE TABLE IF NOT EXISTS `oai_sets` (
  `id` int NOT NULL AUTO_INCREMENT,
  `identify_id` int DEFAULT NULL,
  `set_spec` varchar(255) DEFAULT NULL,
  `set_name` text,
  `set_description` text,
  PRIMARY KEY (`id`),
  KEY `identify_id` (`identify_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- --------------------------------------------------------

--
-- Table structure for table `oai_triples`
--

DROP TABLE IF EXISTS `oai_triples`;
CREATE TABLE IF NOT EXISTS `oai_triples` (
  `id` int NOT NULL AUTO_INCREMENT,
  `record_id` int DEFAULT NULL,
  `property` varchar(100) DEFAULT NULL,
  `value` text,
  PRIMARY KEY (`id`),
  KEY `record_id` (`record_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
COMMIT;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
