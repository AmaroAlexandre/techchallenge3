-- o que eu rodei no Athena (database techchallenge_3)
-- crawler apontando pro gold no S3

-- visao geral
SELECT * FROM techchallenge_3.overview ORDER BY survey_year;


-- 1. mercado
SELECT survey_year, job_family, professionals, share_pct
FROM techchallenge_3.market_structure
WHERE survey_year = 2025
ORDER BY professionals DESC;

-- BI cai, ML e analytics engineer sobem
SELECT survey_year, job_family, share_pct
FROM techchallenge_3.market_structure
WHERE job_family IN ('Analista de BI', 'Engenheiro de ML/IA', 'Analytics Engineer', 'Gestão')
ORDER BY job_family, survey_year;


-- 2. salario
SELECT job_family, avg_salary, n
FROM techchallenge_3.salary_role_seniority
WHERE survey_year = 2025 AND seniority_group = 'Sênior'
ORDER BY avg_salary DESC;

-- junior / pleno / senior
SELECT job_family, seniority_group, n, avg_salary
FROM techchallenge_3.salary_role_seniority
WHERE survey_year = 2025
  AND job_family IN ('Analista de Dados', 'Engenharia de Dados', 'Cientista de Dados')
  AND seniority_group IN ('Júnior', 'Pleno', 'Sênior')
ORDER BY job_family, seniority_group;

-- staff+ so aparece de verdade em 2025
SELECT job_family, n, avg_salary
FROM techchallenge_3.salary_role_seniority
WHERE survey_year = 2025 AND seniority_group = 'Especialista/staff+'
ORDER BY avg_salary DESC;


-- 3. genero
SELECT survey_year, gender_group, n, avg_salary, pct_senior
FROM techchallenge_3.gender_gap
ORDER BY survey_year, gender_group;

-- gap 2025
SELECT
  max(case when gender_group = 'Feminino' then n end) as n_f,
  max(case when gender_group = 'Masculino' then n end) as n_m,
  max(case when gender_group = 'Feminino' then avg_salary end) as salario_f,
  max(case when gender_group = 'Masculino' then avg_salary end) as salario_m,
  max(case when gender_group = 'Feminino' then pct_senior end) as senior_f,
  max(case when gender_group = 'Masculino' then pct_senior end) as senior_m
FROM techchallenge_3.gender_gap
WHERE survey_year = 2025;


-- 4. ferramentas
SELECT technology, adoption_pct, n
FROM techchallenge_3.tech_adoption
WHERE survey_year = 2025
ORDER BY adoption_pct DESC;

-- python passa sql; aws passa azure
SELECT survey_year, technology, adoption_pct
FROM techchallenge_3.tech_adoption
WHERE technology IN ('Python', 'SQL', 'AWS', 'Azure', 'Databricks', 'Power BI', 'Tableau')
ORDER BY technology, survey_year;


-- 5. ia
SELECT job_family, round(sum(pct_uses_genai * n) / sum(n), 1) AS pct_genai, sum(n) AS n
FROM techchallenge_3.ai_adoption
WHERE survey_year = 2025
GROUP BY job_family
ORDER BY pct_genai DESC;


-- 6. regiao e modelo de trabalho
SELECT region_group, sum(n) AS n
FROM techchallenge_3.region_work_model
WHERE survey_year = 2025
GROUP BY region_group
ORDER BY n DESC;

SELECT work_model_group, sum(n) AS n
FROM techchallenge_3.region_work_model
WHERE survey_year = 2025
GROUP BY work_model_group
ORDER BY n DESC;

-- remoto paga mais nesta amostra (o escritorio concentra junior)
SELECT work_model_group, sum(n) AS n,
       round(sum(avg_salary * n) / sum(n), 0) AS salario
FROM techchallenge_3.region_work_model
WHERE survey_year = 2025 AND avg_salary IS NOT NULL
GROUP BY work_model_group
ORDER BY salario DESC;

-- sudeste: remoto vs presencial
SELECT work_model_group, n, avg_salary
FROM techchallenge_3.region_work_model
WHERE survey_year = 2025 AND region_group = 'Sudeste'
ORDER BY n DESC;


-- 7. onde o talento esta
SELECT sector, n, avg_salary
FROM techchallenge_3.sector
WHERE survey_year = 2025
  AND sector IS NOT NULL AND trim(sector) <> ''
ORDER BY n DESC
LIMIT 10;
