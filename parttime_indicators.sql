/*�Ƹ�����Ʈ�� ���� ��ǥ */

--1. ����� �ð� Ȯ��
SELECT LEFT(CONVERT(VARCHAR, [col1], 112), 8) AS 'DATE', A.USERID, B.NAME, A.[col3], A.[col4]
FROM [Database1] A INNER JOIN [Database2] B ON A.USERID = B.ID
WHERE [condition] AND LEFT(CONVERT(VARCHAR, [col1], 112), 8) = convert(date, getdate())
ORDER BY 'DATE' DESC, A.USERID;

--2. �Ϻ� �۾� ����
SELECT LEFT(CONVERT(VARCHAR, [col1], 112), 8) AS 'DATE', A.USERID, B.NAME, COUNT(DISTINCT A.[col2]) AS 'CNT'
FROM [Database1] A INNER JOIN [Database2] B ON A.USERID = B.ID
WHERE [condition] AND LEFT(CONVERT(VARCHAR, [col1], 112), 8) = convert(date, getdate())
GROUP BY A.[col2], B.[col3], LEFT(CONVERT(VARCHAR, [col1], 112), 8)
ORDER BY 'DATE' DESC, A.USERID;

--3. �����ð� ���� Ȯ��
SELECT LEFT(CONVERT(VARCHAR, [col1], 112), 8) AS 'DATE', A.USERID, B.NAME, DATEDIFF(MINUTE, A.[col1], A.[col2]) AS 'TIME'
FROM [Database1] A INNER JOIN [Database2] B ON A.USERID = B.ID
WHERE DATEDIFF(MINUTE, A.[col1], A.[col2]) < 480
ORDER BY 'DATE' DESC, A.[col3];

--4. �Ǵ� �۾� �ð�
SELECT LEFT(CONVERT(VARCHAR, STARTDATETIME, 112), 8) AS 'DATE', A.USERID, B.NAME, AVG(DATEDIFF(MS, A.[col1], A.[col2]))/60000 AS 'AVG'
FROM [Database1] A INNER JOIN [Database2] B ON A.USERID = B.ID
-- (���ø� ���� ���� �� �߰�) WHERE [condition] AND LEFT(CONVERT(VARCHAR, STARTDATETIME, 112), 8) = convert(date, getdate())
GROUP BY A.[col1], B.[col2], LEFT(CONVERT(VARCHAR, STARTDATETIME, 112), 8)
ORDER BY 'DATE' DESC, A.[col1];

--5. �ܾ��� �ľ�
SELECT [col1], COUNT(*)
FROM [Database]
WHERE [condition]
GROUP BY [col1]
ORDER BY 1
