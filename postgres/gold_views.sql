-- 1. Average similarity score per source molecule.
CREATE OR REPLACE VIEW gold.average_similarity_score AS
    SELECT
        source_chembl_id,
        ROUND(AVG(tanimoto_score)::numeric, 4) AS average_tanimoto_score
    FROM gold.fact_similarity
    GROUP BY source_chembl_id;


-- 2. Average deviation of alogp between each source molecule and its
--    top-10 similar (target) molecules.
CREATE OR REPLACE VIEW gold.similarity_property_deviation AS
    SELECT
        fs.source_chembl_id,
        sp.alogp AS source_alogp,
        ROUND(AVG(ABS(tp.alogp - sp.alogp))::numeric, 4) AS avg_alogp_deviation
    FROM gold.fact_similarity fs
    JOIN gold.dim_molecule sp ON fs.source_chembl_id = sp.chembl_id
    JOIN gold.dim_molecule tp ON fs.target_chembl_id = tp.chembl_id
    GROUP BY fs.source_chembl_id, sp.alogp;


-- 3. Pivot of 10 randomly chosen source molecules: rows = target molecule,
--    columns = source molecule, cells = Tanimoto score. The 10 chembl_ids
--    were picked once via
--    `SELECT DISTINCT source_chembl_id FROM gold.fact_similarity ORDER BY RANDOM() LIMIT 10;`
--    and hardcoded, since a view's column list must be static.
CREATE OR REPLACE VIEW gold.similarity_pivot_10_sources AS
    SELECT
        target_chembl_id,
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL85') AS "CHEMBL85",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL553') AS "CHEMBL553",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL940') AS "CHEMBL940",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL192') AS "CHEMBL192",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL112') AS "CHEMBL112",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL42') AS "CHEMBL42",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL1200699') AS "CHEMBL1200699",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL83') AS "CHEMBL83",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL1399') AS "CHEMBL1399",
        MAX(tanimoto_score) FILTER (WHERE source_chembl_id = 'CHEMBL419213') AS "CHEMBL419213"
    FROM gold.fact_similarity
    WHERE source_chembl_id IN (
        'CHEMBL85', 'CHEMBL553', 'CHEMBL940',
        'CHEMBL192', 'CHEMBL112', 'CHEMBL42',
        'CHEMBL1200699', 'CHEMBL83',
        'CHEMBL1399', 'CHEMBL419213'
    )
    GROUP BY target_chembl_id;


-- 4. Neighbor chain: Displays all target rank rows for each source molecule,
--    along with the next most similar target molecule (via LEAD) and the
--    source's 2nd most similar target molecule across all rows.
CREATE OR REPLACE VIEW gold.similarity_neighbor_chain AS
SELECT
    source_chembl_id,
    target_chembl_id,
    tanimoto_score,
    LEAD(target_chembl_id) OVER (
        PARTITION BY source_chembl_id ORDER BY rank
    ) AS next_most_similar_target_chembl_id,
    MAX(target_chembl_id) FILTER (WHERE rank = 2) OVER (
        PARTITION BY source_chembl_id
    ) AS second_most_similar_target_chembl_id
FROM gold.fact_similarity;


-- 5. Average similarity score grouped by: (i) source molecule,
--    (ii) source's aromatic_rings + heavy_atoms, (iii) source's heavy_atoms,
--    and (iv) whole dataset using GROUPING SETS.
--    Individual GROUPING() checks catch rolled-up NULLs across subgroups
--    and the grand-total row, replacing them with 'TOTAL'.
CREATE OR REPLACE VIEW gold.average_similarity_grouped AS
SELECT
    CASE
        WHEN GROUPING(fs.source_chembl_id) = 1 THEN 'TOTAL'
        ELSE fs.source_chembl_id
    END AS source_chembl_id,
    CASE
        WHEN GROUPING(dm.aromatic_rings) = 1 THEN 'TOTAL'
        ELSE dm.aromatic_rings::text
    END AS aromatic_rings,
    CASE
        WHEN GROUPING(dm.heavy_atoms) = 1 THEN 'TOTAL'
        ELSE dm.heavy_atoms::text
    END AS heavy_atoms,
    ROUND(AVG(fs.tanimoto_score)::numeric, 4) AS average_tanimoto_score
FROM gold.fact_similarity fs
JOIN gold.dim_molecule dm ON dm.chembl_id = fs.source_chembl_id
GROUP BY GROUPING SETS (
    (fs.source_chembl_id),
    (dm.aromatic_rings, dm.heavy_atoms),
    (dm.heavy_atoms),
    ()
);
