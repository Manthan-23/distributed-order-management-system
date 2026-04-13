import { Router } from "express";
import * as controller from "../controllers/appController.js";


const router = Router();


router.route("/products").get(controller.getProducts);



export default router;