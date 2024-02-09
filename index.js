const {
  createClient,
  AggregateSteps,
  AggregateGroupByReducers,
} = require("redis");
const { Repository, Schema } = require("redis-om");

const redis = createClient({
  url: "redis://localhost:6379",
});
redis.on("error", (err) => console.log("Redis Client Error", err));
redis.on("connect", () => console.log("Redis Connected"));

const userSchema = new Schema("user", {
  origin: {
    type: "string",
  },
  module: {
    type: "string",
  },
  id: {
    type: "number",
  },
});

const userRepository = new Repository(userSchema, redis);
const modules = ["module1", "module2", "module3"];

const generateData = async () => {
  console.log("Generating Data");
  const randomString = (length) => {
    const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    let result = "";
    const charactersLength = characters.length;
    for (let i = 0; i < length; i++) {
      result += characters.charAt(Math.floor(Math.random() * charactersLength));
    }
    return result;
  };

  const randomInt = (min, max) => {
    return Math.floor(Math.random() * (max - min + 1) + min);
  };

  const numRows = 30000;
  const numOrigin = 600;
  const numModule = 3;

  const numId = 25000;

  const origins = ["localhost.com"].concat(
    Array.from({ length: numOrigin - 1 }, (_) => `${randomString(6)}.com`)
  );

  const ids = [-1].concat(Array.from({ length: numId - 1 }, (_, i) => i));

  for (let i = 0; i < numRows; i++) {
    const origin = origins[randomInt(0, numOrigin - 1)];
    const module = modules[randomInt(0, numModule - 1)];
    const id = ids[randomInt(0, numId - 1)];
    await userRepository.save({ origin, module, id });
  }
};

const pagination = async ({ offset = 0, limit = 10, search = "" }) => {
  const { results } = await redis.ft.aggregate(
    "user:index",
    search ? `@origin:{ *${search}* }` : "*",
    {
      STEPS: [
        {
          type: AggregateSteps.GROUPBY,
          properties: ["@origin"],
          REDUCE: [
            {
              type: AggregateGroupByReducers.COUNT,
              AS: "total",
            },
          ],
        },
        {
          type: AggregateSteps.SORTBY,
          BY: ["@total", "DESC"],
        },
        {
          type: AggregateSteps.LIMIT,
          from: offset,
          size: limit,
        },
      ],
    }
  );

  const origins = results.map((result) => result.origin);

  const paginationRes = await Promise.all(
    origins.map(async (origin) => {
      const module1 = await userRepository
        .search()
        .where("origin")
        .eq(origin)
        .and("module")
        .eq("module1")
        .count();

      const module2 = await userRepository
        .search()
        .where("origin")
        .eq(origin)
        .and("module")
        .eq("module2")
        .count();

      const module3 = await userRepository
        .search()
        .where("origin")
        .eq(origin)
        .and("module")
        .eq("module3")
        .count();

      //-1 == null
      const totalIdnull = await userRepository
        .search()
        .where("origin")
        .eq(origin)
        .and("id")
        .eq(-1)
        .count();

      const { results: resultss } = await redis.ft.aggregate(
        "user:index",
        `@origin:{ ${origin.replace(/\./g, "\\.")} }`,
        {
          STEPS: [
            {
              type: AggregateSteps.GROUPBY,
              properties: ["@id", "@origin"],
              REDUCE: [],
            },
          ],
        }
      );

      return {
        origin,
        module1,
        module2,
        module3,
        uniqueIds: resultss.filter((result) => result.origin === origin).length,
        totalIdnull,
      };
    })
  );

  return paginationRes;
};

const start = async () => {
  await redis.connect();
  // await redis.flushAll();
  await userRepository.createIndex();
  // await generateData();

  const startTime = new Date().getTime();
  const paginationResult = await pagination({
    offset: 0,
    limit: 25,
    search: "",
  });
  console.log("Time taken: ", new Date().getTime() - startTime);

  console.log(paginationResult.length);
};

start();

